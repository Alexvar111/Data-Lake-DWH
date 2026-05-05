DELETE FROM staging.stg_candles_raw 
WHERE trade_date = CAST('{{ ds }}' AS DATE);