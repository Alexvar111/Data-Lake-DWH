import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode, min, max, sum, first, last, lit


def get_spark_session():
    return SparkSession.builder \
        .appName("Moex_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def process_candles(spark, target_date, tickers):
    jdbc_url = "jdbc:postgresql://postgres-dwh:5432/dwh_db"
    db_properties = {"user": "data_eng", "password": "data_eng_pwd", "driver": "org.postgresql.Driver"}

    for ticker in tickers:
        s3_path = f"s3a://moex-bronze/raw_api/{ticker}/{target_date}/{ticker}_candles.json"

        try:
            print(f"Читаем данные для {ticker} из {s3_path}")
            df_raw = spark.read.json(s3_path)

            # 1. Распределенный парсинг массивов JSON (The Spark Way)
            df_exploded = df_raw.select(explode(col("candles.data")).alias("arr"))

            if df_exploded.isEmpty():
                print(f"Нет данных для {ticker} на дату {target_date}")
                continue

            # 2. Вытаскиваем колонки (индексы API: 0=open, 1=close, 2=high, 3=low, 5=vol, 6=begin)
            df_parsed = df_exploded.select(
                col("arr")[0].cast("double").alias("open"),
                col("arr")[1].cast("double").alias("close"),
                col("arr")[2].cast("double").alias("high"),
                col("arr")[3].cast("double").alias("low"),
                col("arr")[5].cast("long").alias("volume"),
                col("arr")[6].cast("timestamp").alias("candle_ts")
            )

            # 3. АГРЕГАЦИЯ: Сворачиваем 500+ минутных свечей в одну дневную
            df_silver = df_parsed \
                .withColumn("trade_date", to_date(col("candle_ts"))) \
                .filter(col("trade_date") == lit(target_date).cast("date")) \
                .orderBy("candle_ts") \
                .groupBy("trade_date") \
                .agg(
                first("open").alias("open_price"),  # Открытие первой минуты
                last("close").alias("close_price"),  # Закрытие последней минуты
                max("high").alias("high_price"),  # Абсолютный максимум за день
                min("low").alias("low_price"),  # Абсолютный минимум за день
                sum("volume").alias("volume")  # Суммарный объем торгов
            ) \
                .withColumn("ticker", lit(ticker))

            print(f"Загружаем {ticker} (агрегировано) в PostgreSQL Staging...")

            # Spark сам создаст таблицу, если ее нет
            df_silver.write.jdbc(
                url=jdbc_url,
                table="staging.stg_candles_raw",
                mode="append",
                properties=db_properties
            )
            print(f"Успешно загружено для {ticker}")

        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"⚠️ Файл не найден для {ticker}")
                continue
            else:
                print(f"❌ ОШИБКА: {e}")
                raise e


def process_emitents(spark, target_date):
    jdbc_url = "jdbc:postgresql://postgres-dwh:5432/dwh_db"
    db_properties = {"user": "data_eng", "password": "data_eng_pwd", "driver": "org.postgresql.Driver"}

    # Меняем путь
    s3_path = f"s3a://moex-bronze/refs/emitents/{target_date}/emitents.parquet"

    try:
        print(f"Читаем справочники из {s3_path}")
        # Читаем как Parquet
        df_refs = spark.read.parquet(s3_path)

        df_refs.write.jdbc(
            url=jdbc_url,
            table="staging.stg_emitents_raw",
            mode="overwrite",
            properties=db_properties
        )
    except Exception as e:
        print(f"Ошибка справочников: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(1)

    target_date = sys.argv[1]
    tickers = sys.argv[2].split(",")

    spark = get_spark_session()
    process_emitents(spark, target_date)
    process_candles(spark, target_date, tickers)
    spark.stop()