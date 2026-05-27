DELETE FROM staging.stg_candles_raw 
WHERE trade_date = CAST('{{ macros.ds_add(ds, -1) }}' AS DATE);