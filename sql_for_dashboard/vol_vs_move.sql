WITH d AS (
  SELECT date, company, open, close, volume,
         ABS(close - open) / NULLIF(open,0) AS move_pct
  FROM public.quotes
)
SELECT * FROM d WHERE move_pct IS NOT NULL;
