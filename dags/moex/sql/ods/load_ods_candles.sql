INSERT INTO ods.candles (
    ticker, trade_date, open_price, close_price, high_price, low_price, volume
)
SELECT 
    ticker, 
    trade_date, 
    open_price, 
    close_price, 
    high_price, 
    low_price, 
    volume
FROM staging.stg_candles_raw
WHERE trade_date = CAST('{{ macros.ds_add(ds, -1) }}' AS DATE)

-- Если такая акция за эту дату уже есть в ODS, мы обновляем её значения
ON CONFLICT (ticker, trade_date) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    close_price = EXCLUDED.close_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    volume = EXCLUDED.volume,
    load_dt = CURRENT_TIMESTAMP; -- Фиксируем время обновления