-- Файл: dags/moex/sql/dds/load_dds_emitents_scd2.sql

WITH source_data AS (
    -- 1. Берем данные из Staging и считаем MD5 хэш по всем бизнес-атрибутам
    -- Это нужно, чтобы понять, изменилось ли что-то
    SELECT 
        ticker,
        company_name,
        sector,
        dividend_policy_active,
        MD5(CONCAT_WS('||', company_name, sector, dividend_policy_active::TEXT)) AS row_hash
    FROM staging.stg_emitents_raw
),
current_dds AS (
    -- 2. Берем только актуальные записи из DDS и тоже считаем их хэш
    SELECT 
        emitent_id,
        ticker,
        company_name,
        sector,
        dividend_policy_active,
        valid_from,
        valid_to,
        is_current,
        MD5(CONCAT_WS('||', company_name, sector, dividend_policy_active::TEXT)) AS row_hash
    FROM dds.dim_emitents
    WHERE is_current = TRUE
),
changed_records AS (
    -- 3. Находим тех, кто изменился
    SELECT 
        s.ticker
    FROM source_data s
    INNER JOIN current_dds c ON s.ticker = c.ticker
    WHERE s.row_hash != c.row_hash
),
update_old_records AS (
    -- 4. Закрываем старые версии изменившихся записей (UPDATE)
    UPDATE dds.dim_emitents
    SET 
        valid_to = CURRENT_TIMESTAMP, -- Ставим текущее время как конец действия
        is_current = FALSE            -- Снимаем флаг актуальности
    WHERE ticker IN (SELECT ticker FROM changed_records)
      AND is_current = TRUE
    RETURNING emitent_id
)
-- 5. Вставляем новые записи (как абсолютно новые тикеры, так и новые версии измененных)
INSERT INTO dds.dim_emitents (
    ticker, company_name, sector, dividend_policy_active, valid_from, valid_to, is_current
)
SELECT 
    s.ticker,
    s.company_name,
    s.sector,
    s.dividend_policy_active,
    -- МАГИЯ ТУТ: Если новая запись - "начало времен", если изменение - "сейчас"
    CASE WHEN c.ticker IS NULL THEN '1900-01-01 00:00:00'::TIMESTAMP ELSE CURRENT_TIMESTAMP END AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP AS valid_to, 
    TRUE AS is_current
FROM source_data s
LEFT JOIN current_dds c ON s.ticker = c.ticker
WHERE c.ticker IS NULL 
   OR s.ticker IN (SELECT ticker FROM changed_records);