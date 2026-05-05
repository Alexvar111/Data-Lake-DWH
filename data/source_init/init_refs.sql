CREATE TABLE IF NOT EXISTS emitents (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100),
    sector VARCHAR(50),
    dividend_policy_active BOOLEAN
);

INSERT INTO emitents (ticker, company_name, sector, dividend_policy_active) VALUES
('SBER', 'Сбербанк', 'Финансы', true),
('GAZP', 'Газпром', 'Нефтегаз', true),
('MOEX', 'Московская биржа', 'Финансы', true),
('YDEX', 'Яндекс', 'IT', false),
('LKOH', 'Лукойл', 'Нефтегаз', true),
('ROSN', 'Роснефть', 'Нефтегаз', true),
('NVTK', 'Новатэк', 'Нефтегаз', true),
('MGNT', 'Магнит', 'Ритейл', true),
('TCSG', 'ТКС Холдинг', 'Финансы', false),
('PLZL', 'Полюс', 'Добыча', true),
('CHMF', 'Северсталь', 'Металлургия', true),
('MTSS', 'МТС', 'Телеком', true),
('ALRS', 'Алроса', 'Добыча', true),
('VTBR', 'ВТБ', 'Финансы', false),
('TATN', 'Татнефть', 'Нефтегаз', true),
('GMKN', 'Норникель', 'Металлургия', true),
('NLMK', 'НЛМК', 'Металлургия', true),
('IRAO', 'Интер РАО', 'Энергетика', true),
('RTKM', 'Ростелеком', 'Телеком', true),
('AFLT', 'Аэрофлот', 'Транспорт', false),
('PIKK', 'ПИК', 'Строительство', false),
('MAGN', 'ММК', 'Металлургия', true),
('CBOM', 'МКБ', 'Финансы', false),
('VKCO', 'VK', 'IT', false),
('TRNFP', 'Транснефть', 'Нефтегаз', true),
('PHOR', 'Фосагро', 'Химия', true),
('AFKS', 'АФК Система', 'Инвестиции', true),
('ENPG', 'EN+ Group', 'Энергетика', true),
('UPRO', 'Юнипро', 'Энергетика', true),
('HYDR', 'РусГидро', 'Энергетика', true)
ON CONFLICT (ticker) DO NOTHING;