FROM apache/airflow:2.9.3-python3.11

USER root

# Устанавливаем Java и wget
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre-headless \
         wget \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Скачиваем и распаковываем Spark для воркера Airflow
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    && tar -xvzf spark-3.5.1-bin-hadoop3.tgz -C /opt/ \
    && rm spark-3.5.1-bin-hadoop3.tgz \
    && ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark

# Прописываем пути
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"