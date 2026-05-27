ALTER TABLE analytics.dm_stock_analytics 
REPLACE PARTITION '{{ macros.ds_add(ds, -1) }}' 
FROM analytics.dm_stock_analytics_buffer;