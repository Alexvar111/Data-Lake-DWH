import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DAGS_FOLDER = '/opt/airflow/dags'

SILVER_READY = Dataset('s3://moex-bronze/silver_complete')
GOLD_READY = Dataset('s3://moex-bronze/gold_complete')

with DAG(
        dag_id='03_moex_dwh_gold',
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule=[SILVER_READY],
        catchup=False,
        max_active_runs=1,
        tags=['moex', 'gold', 'ods', 'dds', 'dm'],
        template_searchpath=[os.path.join(DAGS_FOLDER, 'moex', 'sql')]
) as dag:
    # Загружаем сырые факты в ODS
    load_ods_candles = SQLExecuteQueryOperator(
        task_id='load_ods_candles',
        conn_id='postgres_dwh_conn',
        sql='ods/load_ods_candles.sql',
    )
    #Data Quality Check
    # Запрос ищет "плохие" строки. Если их количество равно 0, проверка пройдена (True).
    dq_check_ods = SQLCheckOperator(
        task_id='dq_check_candle_logic',
        conn_id='postgres_dwh_conn',
        sql='dq/check_ods_candles.sql',
    )

    # Загружаем справочники (SCD2)
    load_dds_emitents = SQLExecuteQueryOperator(
        task_id='load_dds_emitents',
        conn_id='postgres_dwh_conn',
        sql='dds/load_dds_emitents_scd2.sql'
    )

    # Сборка витрины
    build_dm_candles = SQLExecuteQueryOperator(
        task_id='build_dm_candles',
        conn_id='postgres_dwh_conn',
        sql='dm/build_dm_candles.sql',
        outlets=[GOLD_READY],
    )

    load_ods_candles >> dq_check_ods >> load_dds_emitents >> build_dm_candles