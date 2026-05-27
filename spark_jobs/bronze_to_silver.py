import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, max, min, sum, lit, row_number, when
from pyspark.sql.window import Window

JDBC_URL = "jdbc:postgresql://postgres-dwh:5432/dwh_db"


def get_spark_session(s3_user: str, s3_pwd: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("Moex_Bronze_to_Silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", s3_user)
        .config("spark.hadoop.fs.s3a.secret.key", s3_pwd)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def _parse_candles_df(df_raw: DataFrame, ticker: str, process_date: str) -> DataFrame:
    """Разбирает сырой JSONL-файл свечей в агрегированную строку OHLCV."""
    df_prepared = (
        df_raw
        .select(
            col("begin").cast("timestamp").alias("begin_ts"),
            col("open").cast("double").alias("open"),
            col("close").cast("double").alias("close"),
            col("high").cast("double").alias("high"),
            col("low").cast("double").alias("low"),
            col("volume").cast("long").alias("volume"),
        )
        .withColumn("trade_date", to_date(col("begin_ts")))
        .filter(col("trade_date") == lit(process_date).cast("date"))
    )

    w_open = Window.partitionBy("trade_date").orderBy(col("begin_ts").asc())
    w_close = Window.partitionBy("trade_date").orderBy(col("begin_ts").desc())

    df_ranked = (
        df_prepared
        .withColumn("rn_open", row_number().over(w_open))
        .withColumn("rn_close", row_number().over(w_close))
    )

    df_open_close = (
        df_ranked
        .groupBy("trade_date")
        .agg(
            max(when(col("rn_open") == 1, col("open"))).alias("open_price"),
            max(when(col("rn_close") == 1, col("close"))).alias("close_price"),
        )
    )

    df_hlv = (
        df_prepared
        .groupBy("trade_date")
        .agg(
            max("high").alias("high_price"),
            min("low").alias("low_price"),
            sum("volume").alias("volume"),
        )
    )

    return (
        df_open_close
        .join(df_hlv, on="trade_date", how="inner")
        .withColumn("ticker", lit(ticker))
    )


def _candidate_dates(target_date: str):
    d = datetime.strptime(target_date, "%Y-%m-%d").date()
    # Для dataset-triggered запуска сначала пробуем текущую логическую дату,
    # затем fallback на предыдущий день (часто именно он загружен в bronze).
    return [d.strftime("%Y-%m-%d"), (d - timedelta(days=1)).strftime("%Y-%m-%d")]


def _read_ticker(spark: SparkSession, ticker: str, target_date: str):
    """Возвращает (DataFrame, source_date) или (None, None), если файл отсутствует."""
    for source_date in _candidate_dates(target_date):
        s3_path = f"s3a://moex-bronze/raw_api/{ticker}/{source_date}/{ticker}_candles.jsonl"
        try:
            return spark.read.json(s3_path), source_date
        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"[WARN] Файл не найден: {s3_path}")
                continue
            raise
    return None, None


def process_candles(spark: SparkSession, target_date: str, tickers: list, db_properties: dict):
    """
    Читает файлы всех тикеров параллельно через union, затем одним
    write.jdbc пишет всё в staging — один проход по кластеру вместо N.
    """
    frames = []
    for ticker in tickers:
        df_raw, source_date = _read_ticker(spark, ticker, target_date)
        if df_raw is None:
            continue
        df_agg = _parse_candles_df(df_raw, ticker, source_date)
        if not df_agg.isEmpty():
            frames.append(df_agg)

    if not frames:
        print(f"[WARN] Нет данных ни по одному тикеру за {target_date}. Пропускаем запись.")
        return

    # union всех тикеров — Spark выполняет их параллельно на executor-ах
    df_all = frames[0]
    for df in frames[1:]:
        df_all = df_all.union(df)

    print(f"Загружаем {len(frames)} тикеров в PostgreSQL Staging одним проходом...")
    df_all.write.jdbc(
        url=JDBC_URL,
        table="staging.stg_candles_raw",
        mode="append",
        properties=db_properties,
    )
    print("Готово.")


def process_emitents(spark: SparkSession, target_date: str, db_properties: dict):
    for source_date in _candidate_dates(target_date):
        s3_path = f"s3a://moex-bronze/refs/emitents/{source_date}/emitents.parquet"
        try:
            print(f"Читаем справочники из {s3_path}")
            df_refs = spark.read.parquet(s3_path)
            df_refs.write.jdbc(
                url=JDBC_URL,
                table="staging.stg_emitents_raw",
                mode="overwrite",
                properties=db_properties,
            )
            return
        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"[WARN] Файл справочников не найден: {s3_path}")
                continue
            print(f"[ERROR] Ошибка загрузки справочников: {e}")
            raise
    raise FileNotFoundError("Не найден parquet справочников ни за target_date, ни за предыдущий день.")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: bronze_to_silver.py <target_date> <ticker1,ticker2,...>")
        sys.exit(1)

    target_date = sys.argv[1]
    tickers = sys.argv[2].split(",")

    pg_user = os.environ['SPARK_PG_USER']
    pg_pwd  = os.environ['SPARK_PG_PASSWORD']
    s3_user = os.environ['SPARK_S3_USER']
    s3_pwd  = os.environ['SPARK_S3_PASSWORD']

    # Добавляем параметры батчинга, иначе Spark будет писать в Postgres по одной строке
    db_properties = {
        "user": pg_user, 
        "password": pg_pwd, 
        "driver": "org.postgresql.Driver",
        "batchsize": "50000",
        "rewriteBatchedStatements": "true"
    }

    spark = get_spark_session(s3_user, s3_pwd)
    process_emitents(spark, target_date, db_properties)
    process_candles(spark, target_date, tickers, db_properties)
    spark.stop()