from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.api_to_s3 import GenericApiToS3Operator
from operators.pg_to_s3 import PostgresToS3Operator
from moex.config.config import TICKERS

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Dataset-маркер, который сигнализирует DAG 02, что бронза готова
BRONZE_READY = Dataset('s3://moex-bronze/ingestion_complete')


def moex_candles_pagination(data: dict, current_params: dict):
    """
    Стратегия пагинации для API Московской Биржи (ISS).
    Возвращает записи свечей и параметры следующей страницы.
    Если данных нет или страница последняя — next_params = None.
    """
    try:
        candles_block = data.get("candles", {})
        records = candles_block.get("data", [])
        metadata = candles_block.get("metadata", {})
        columns = list(metadata.keys())

        # Преобразуем список массивов в список словарей
        parsed = [dict(zip(columns, row)) for row in records]

        if not parsed:
            return [], None

        # Пагинация через offset
        current_start = current_params.get("start", 0)
        page_size = len(records)
        next_params = {**current_params, "start": current_start + page_size}
        return parsed, next_params
    except (KeyError, TypeError):
        return [], None


MOEX_BASE_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities"

with DAG(
    dag_id='01_moex_ingest_bronze',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['moex', 'bronze', 'ingestion']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(
        task_id='end',
        outlets=[BRONZE_READY],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    extract_refs = PostgresToS3Operator(
        task_id='extract_emitents_to_s3',
        sql_query='SELECT * FROM emitents;',
        s3_bucket='moex-bronze',
        s3_key='refs/emitents/{{ ds }}/emitents.parquet',
        pg_conn_id='postgres_source_conn',
        aws_conn_id='minio_s3_conn'
    )

    start >> extract_refs

    # Каждый тикер — отдельная параллельная задача
    for t_info in TICKERS:
        ticker = t_info["ticker"]
        interval = t_info["interval"]

        extract_api = GenericApiToS3Operator(
            task_id=f'extract_api_{ticker}',
            endpoint=f'{MOEX_BASE_URL}/{ticker}/candles.json',
            req_params={
                'interval': interval,
                'from': '{{ data_interval_start | ds }}',
                'till': '{{ data_interval_end | ds }}',
                'start': 0,
            },
            pagination_function=moex_candles_pagination,
            s3_bucket='moex-bronze',
            s3_key=f'raw_api/{ticker}/{{{{ ds }}}}/{ticker}_candles.jsonl',
            aws_conn_id='minio_s3_conn'
        )

        extract_refs >> extract_api >> end