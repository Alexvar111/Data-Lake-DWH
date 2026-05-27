import os
import json
import time
import random
import tempfile
import requests
from typing import Callable, Optional, Dict, Any
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException


class GenericApiToS3Operator(BaseOperator):
    """
    Универсальный оператор для выгрузки данных из любого API и загрузки в S3 (MinIO).
    Поддерживает пагинацию и потоковую запись на диск кусками (защита от OOM).
    """

    template_fields = ('endpoint', 'req_params', 's3_key')

    def __init__(
            self,
            endpoint: str,
            s3_bucket: str,
            s3_key: str,
            aws_conn_id: str = 'minio_s3_conn',
            req_params: Optional[Dict[str, Any]] = None,
            headers: Optional[Dict[str, Any]] = None,
            pagination_function: Optional[Callable] = None,
            request_timeout: int = 30,
            max_request_retries: int = 5,
            backoff_base_seconds: float = 1.0,
            backoff_max_seconds: float = 30.0,
            retryable_status_codes: tuple = (429, 500, 502, 503, 504),
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.req_params = req_params or {}
        self.headers = headers or {}
        # Функция, которая определяет, как достать массив данных из ответа и как получить параметры следующей страницы
        self.pagination_function = pagination_function
        self.request_timeout = request_timeout
        self.max_request_retries = max_request_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.retryable_status_codes = retryable_status_codes

    def _compute_backoff_seconds(self, attempt: int) -> float:
        exponential = self.backoff_base_seconds * (2 ** (attempt - 1))
        capped = min(exponential, self.backoff_max_seconds)
        jitter = random.uniform(0, capped * 0.25)
        return capped + jitter

    def _parse_retry_after(self, response: requests.Response) -> Optional[float]:
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return None
        try:
            return float(retry_after)
        except ValueError:
            return None

    def _request_with_retries(self, params: Dict[str, Any]) -> requests.Response:
        for attempt in range(1, self.max_request_retries + 1):
            try:
                response = requests.get(
                    self.endpoint,
                    params=params,
                    headers=self.headers,
                    timeout=self.request_timeout,
                )
            except requests.RequestException as err:
                if attempt == self.max_request_retries:
                    raise
                sleep_s = self._compute_backoff_seconds(attempt)
                self.log.warning(
                    f"Сетевая ошибка API ({err}). Повтор {attempt}/{self.max_request_retries} через {sleep_s:.2f} сек."
                )
                time.sleep(sleep_s)
                continue

            if response.status_code in self.retryable_status_codes:
                if attempt == self.max_request_retries:
                    response.raise_for_status()
                retry_after = self._parse_retry_after(response)
                sleep_s = retry_after if retry_after is not None else self._compute_backoff_seconds(attempt)
                self.log.warning(
                    f"API вернуло {response.status_code}. Повтор {attempt}/{self.max_request_retries} через {sleep_s:.2f} сек."
                )
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("Unexpected retry loop termination")

    def _fetch_and_write_chunks(self, tmp_file_path: str) -> int:
        total_records = 0
        current_params = self.req_params.copy()

        # Пишем данные в формате JSONL (каждая запись — отдельный JSON на новой строке).
        # Это позволяет Spark'у читать файлы любого размера без проблем с памятью.
        with open(tmp_file_path, 'w', encoding='utf-8') as f:
            while True:
                self.log.info(f"Запрос к API: {self.endpoint} с параметрами {current_params}")
                response = self._request_with_retries(current_params)
                
                data = response.json()

                # Если передана кастомная функция пагинации, используем её
                if self.pagination_function:
                    records, next_params = self.pagination_function(data, current_params)
                else:
                    # По умолчанию предполагаем, что ответ - это просто список записей, и пагинации нет
                    records = data if isinstance(data, list) else [data]
                    next_params = None

                if not records:
                    break

                # Потоково пишем чанк на диск
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    total_records += 1

                self.log.info(f"Получена страница. Итого выгружено записей: {total_records}")

                if not next_params:
                    break
                current_params = next_params

        return total_records

    def _upload_to_s3(self, tmp_file_path: str):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_file(
            filename=tmp_file_path,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        self.log.info(f"Файл успешно сохранен в S3 по пути: s3://{self.s3_bucket}/{self.s3_key}")

    def _cleanup(self, tmp_file_path: str):
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
            self.log.info("Временный файл очищен.")

    def execute(self, context):
        # Формируем временный файл
        with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            # 1. Тянем данные с учетом пагинации и сразу пишем на диск
            total_records = self._fetch_and_write_chunks(tmp_file_path)

            # 2. Проверяем, есть ли данные
            if total_records == 0:
                self.log.warning("API не вернуло данных. Загрузка пропущена.")
                raise AirflowSkipException("Нет данных для выгрузки. Пропускаем таску.")

            # 3. Грузим в S3
            self.log.info(f"Выгрузка завершена. Всего записей: {total_records}. Грузим в S3...")
            self._upload_to_s3(tmp_file_path)

        finally:
            # 4. Убираем за собой
            self._cleanup(tmp_file_path)
