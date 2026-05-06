import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode, min, max, sum, first, last, lit


# 1. Функция создания сессии теперь принимает логин и пароль от S3
def get_spark_session(s3_user, s3_pwd):
    return SparkSession.builder \
        .appName("Moex_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", s3_user) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_pwd) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


# 2. Передаем креды от Postgres аргументом db_properties
def process_candles(spark, target_date, tickers, db_properties):
    jdbc_url = "jdbc:postgresql://postgres-dwh:5432/dwh_db"

    for ticker in tickers:
        s3_path = f"s3a://moex-bronze/raw_api/{ticker}/{target_date}/{ticker}_candles.json"

        try:
            print(f"Читаем данные для {ticker} из {s3_path}")
            df_raw = spark.read.json(s3_path)

            df_exploded = df_raw.select(explode(col("candles.data")).alias("arr"))

            if df_exploded.isEmpty():
                print(f"Нет данных для {ticker} на дату {target_date}")
                continue

            df_parsed = df_exploded.select(
                col("arr")[0].cast("double").alias("open"),
                col("arr")[1].cast("double").alias("close"),
                col("arr")[2].cast("double").alias("high"),
                col("arr")[3].cast("double").alias("low"),
                col("arr")[5].cast("long").alias("volume"),
                col("arr")[6].cast("timestamp").alias("candle_ts")
            )

            df_silver = df_parsed \
                .withColumn("trade_date", to_date(col("candle_ts"))) \
                .filter(col("trade_date") == lit(target_date).cast("date")) \
                .orderBy("candle_ts") \
                .groupBy("trade_date") \
                .agg(
                first("open").alias("open_price"),
                last("close").alias("close_price"),
                max("high").alias("high_price"),
                min("low").alias("low_price"),
                sum("volume").alias("volume")
            ) \
                .withColumn("ticker", lit(ticker))

            print(f"Загружаем {ticker} (агрегировано) в PostgreSQL Staging...")

            df_silver.write.jdbc(
                url=jdbc_url,
                table="staging.stg_candles_raw",
                mode="append",
                properties=db_properties  # <- Используем переданные креды
            )
            print(f"Успешно загружено для {ticker}")

        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"⚠️ Файл не найден для {ticker}")
                continue
            else:
                print(f"❌ ОШИБКА: {e}")
                raise e


def process_emitents(spark, target_date, db_properties):
    jdbc_url = "jdbc:postgresql://postgres-dwh:5432/dwh_db"
    s3_path = f"s3a://moex-bronze/refs/emitents/{target_date}/emitents.parquet"

    try:
        print(f"Читаем справочники из {s3_path}")
        df_refs = spark.read.parquet(s3_path)

        df_refs.write.jdbc(
            url=jdbc_url,
            table="staging.stg_emitents_raw",
            mode="overwrite",
            properties=db_properties  # <- Используем переданные креды
        )
    except Exception as e:
        print(f"Ошибка справочников: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 7:  # <- Теперь ждем 7 аргументов
        sys.exit(1)

    target_date = sys.argv[1]
    tickers = sys.argv[2].split(",")

    # 3. Ловим секреты из аргументов
    pg_user = sys.argv[3]
    pg_pwd = sys.argv[4]
    s3_user = sys.argv[5]
    s3_pwd = sys.argv[6]

    db_properties = {"user": pg_user, "password": pg_pwd, "driver": "org.postgresql.Driver"}

    spark = get_spark_session(s3_user, s3_pwd)
    process_emitents(spark, target_date, db_properties)
    process_candles(spark, target_date, tickers, db_properties)
    spark.stop()