# MOEX Data Lakehouse (ETL Pipeline)

Enterprise-уровень пайплайн для сбора, обработки и анализа данных Московской биржи (MOEX). Проект реализует многослойную архитектуру хранилища данных (Medallion Architecture) с использованием современных Data Engineering практик: строгий Batch-процессинг, Data Quality проверки на каждом слое и разделение вычислительных мощностей.

## 🛠 Технологический стек

* **Оркестрация:** Apache Airflow 2.9 (динамическая генерация DAG-ов)
* **Вычисления (ETL):** Apache Spark 3.5 (PySpark)
* **DWH (Ядро):** PostgreSQL 15 (Слои STG, ODS, DDS, DM)
* **Data Lake (S3):** MinIO (Слои Bronze и Export)
* **Аналитика (OLAP):** ClickHouse (Фасадный слой для BI)
* **Инфраструктура:** Docker & Docker Compose

## 🏗 Архитектура потока данных (Pipeline)

1. **Bronze Layer (Ingestion):** Airflow загружает сырые данные из API MOEX (факты) и СУБД PostgreSQL (измерения/справочники) в S3 бакет `moex-bronze`.
2. **Silver Layer (Transformation):** Spark читает сырые данные, выполняет дедубликацию, приведение типов и загружает их в слой Staging DWH (PostgreSQL).
3. **Gold Layer (DWH Core):** SQL-скрипты формируют операционный слой (ODS), детальный слой с историчностью SCD2 (DDS) и денормализованные витрины (DM).
4. **Serving Layer (Export):** Готовые витрины выгружаются обратно в S3 (`moex-export`), откуда ClickHouse забирает их через нативную s3-функцию для молниеносной аналитики.

На каждом этапе реализованы проверки качества данных (Data Quality Gates), которые останавливают пайплайн при обнаружении аномалий или нарушении схемы.

## 🚀 Инструкция по локальному запуску (Windows)

### Предварительные требования
* Установленный **Docker Desktop** (с включенным бэкендом WSL2).
* Git (опционально, для клонирования).

### 1. Подготовка окружения
Клонируйте репозиторий и создайте файл `.env` в корне проекта со следующим содержимым:
```env
# Airflow (На Windows жестко задаем UID)
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_AIRFLOW_WWW_USER_EMAIL=admin@example.com

# PostgreSQL Source (Справочники)
POSTGRES_SOURCE_USER=admin
POSTGRES_SOURCE_PASSWORD=source_pwd
POSTGRES_SOURCE_DB=source_db

# PostgreSQL DWH (Хранилище)
POSTGRES_DWH_USER=data_eng
POSTGRES_DWH_PASSWORD=data_eng_pwd
POSTGRES_DWH_DB=dwh_db

# MinIO (S3)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=supersecretpassword

# ClickHouse
CLICKHOUSE_USER=airflow
CLICKHOUSE_PASSWORD=airflow
CLICKHOUSE_DB=default


