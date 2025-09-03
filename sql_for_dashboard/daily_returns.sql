SELECT
  date,
  company,
  ret
FROM (
  SELECT
    date,
    company,
    close / LAG(close) OVER (PARTITION BY company ORDER BY date) - 1 AS ret
  FROM public.quotes
) t
WHERE ret IS NOT NULL
ORDER BY company, date;
