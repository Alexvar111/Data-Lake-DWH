ALTER TABLE analytics.dm_stock_analytics 
REPLACE PARTITION '{{ ds }}' 
FROM analytics.dm_stock_analytics_buffer;