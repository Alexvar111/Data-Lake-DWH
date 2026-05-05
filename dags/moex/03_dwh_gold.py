import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DAGS_FOLDER = '/opt/airflow/dags'

with DAG(
        dag_id='03_moex_dwh_gold',
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        tags=['moex', 'gold', 'ods', 'dds', 'dm'],
        template_searchpath=[os.path.join(DAGS_FOLDER, 'moex', 'sql')]
) as dag:
    load_ods_candles = SQLExecuteQueryOperator(
        task_id='load_ods_candles',
        conn_id='postgres_dwh_conn',
        sql='ods/load_ods_candles.sql'
    )

    load_dds_emitents = SQLExecuteQueryOperator(
        task_id='load_dds_emitents',
        conn_id='postgres_dwh_conn',
        sql='dds/load_dds_emitents_scd2.sql'
    )

    # НОВАЯ ТАСКА: Сборка витрины
    build_dm_candles = SQLExecuteQueryOperator(
        task_id='build_dm_candles',
        conn_id='postgres_dwh_conn',
        sql='dm/build_dm_candles.sql'
    )

    # Выстраиваем цепочку: Сначала факты -> Потом справочник -> Потом сборка витрины
    load_ods_candles >> load_dds_emitents >> build_dm_candles