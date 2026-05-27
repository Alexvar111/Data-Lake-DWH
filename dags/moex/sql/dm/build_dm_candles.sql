-- 1. Идемпотентность: удаляем данные за сегодня, если они там уже есть
DELETE FROM dm.dm_stock_analytics WHERE trade_date = CAST('{{ macros.ds_add(ds, -1) }}' AS DATE);

-- 2. Собираем витрину
INSERT INTO dm.dm_stock_analytics (
    trade_date, ticker, company_name, sector, is_dividend_payer,
    open_price, close_price, high_price, low_price, volume, daily_return_pct
)
SELECT 
    f.trade_date,
    f.ticker,
    d.company_name,
    d.sector,
    d.dividend_policy_active AS is_dividend_payer,
    f.open_price,
    f.close_price,
    f.high_price,
    f.low_price,
    f.volume,
    -- Считаем процент изменения цены за день
    ROUND(((f.close_price - f.open_price) / f.open_price) * 100, 2) AS daily_return_pct
FROM ods.candles f
-- МАГИЯ ТУТ: Соединяем с учетом исторических интервалов SCD2
JOIN dds.dim_emitents d 
  ON f.ticker = d.ticker
 AND f.trade_date >= CAST(d.valid_from AS DATE) 
 AND f.trade_date < CAST(d.valid_to AS DATE)
WHERE f.trade_date = CAST('{{ macros.ds_add(ds, -1) }}' AS DATE);