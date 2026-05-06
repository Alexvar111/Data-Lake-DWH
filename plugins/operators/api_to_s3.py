import json
import requests
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class MoexApiToS3Operator(BaseOperator):
    """
    Кастомный оператор для выгрузки сырых данных из API MOEX и загрузки в S3 (MinIO).
    """

    template_fields = ('api_start_date', 'api_end_date', 's3_key')

    def __init__(
            self,
            ticker: str,
            interval: int,
            api_start_date: str,
            api_end_date: str,
            s3_bucket: str,
            s3_key: str,
            aws_conn_id: str = 'minio_s3_conn',
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ticker = ticker
        self.interval = interval
        self.api_start_date = api_start_date
        self.api_end_date = api_end_date
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info(f"Запрашиваем API MOEX для {self.ticker} с {self.api_start_date} по {self.api_end_date}")

        # URL для исторических данных по свечам
        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{self.ticker}/candles.json"
        params = {
            "from": self.api_start_date,
            "till": self.api_end_date,
            "interval": self.interval
        }
        headers = {"User-Agent": "Mozilla/5.0 (Lakehouse Pipeline)"}

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        raw_data = response.json()

        # Проверяем, есть ли данные (Circuit Breaker)
        candles = raw_data.get('candles', {}).get('data', [])
        if not candles:
            self.log.warning(f"Нет данных за период {self.api_start_date} - {self.api_end_date}. Загрузка пропущена.")
            return

        self.log.info(f"Получено {len(candles)} записей. Сохраняем в S3...")

        # Превращаем JSON обратно в строку для загрузки
        json_string = json.dumps(raw_data, ensure_ascii=False)

        # Используем S3Hook для подключения к MinIO
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # Загружаем строку как файл в бакет
        s3_hook.load_string(
            string_data=json_string,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

        self.log.info(f"Файл успешно сохранен в S3 по пути: s3://{self.s3_bucket}/{self.s3_key}")