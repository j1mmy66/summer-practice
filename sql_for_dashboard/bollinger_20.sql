WITH b AS (
  SELECT
    date, company, close,
    AVG(close)    OVER (PARTITION BY company ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma20,
    STDDEV_SAMP(close) OVER (PARTITION BY company ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sd20
  FROM public.quotes
)
SELECT
  date, company, close,
  sma20,
  sma20 + 2*sd20 AS upper_band,
  sma20 - 2*sd20 AS lower_band
FROM b;
