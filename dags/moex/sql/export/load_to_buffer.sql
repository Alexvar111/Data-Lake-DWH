INSERT INTO analytics.dm_stock_analytics_buffer
SELECT * FROM s3(
    'http://minio:9000/moex-export/dm_stock_analytics/{{ params.proc_ds }}.parquet',
    'admin',
    'supersecretpassword',
    'Parquet'
);