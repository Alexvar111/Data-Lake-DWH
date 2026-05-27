SELECT COUNT(*) = 0 
FROM ods.candles 
WHERE trade_date = '{{ macros.ds_add(ds, -1) }}'
  AND (
      high_price < low_price 
      OR volume < 0 
      OR open_price <= 0 
      OR close_price <= 0
  );