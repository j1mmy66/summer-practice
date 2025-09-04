WITH ch AS (
  SELECT date, company,
         close - LAG(close) OVER (PARTITION BY company ORDER BY date) AS diff
  FROM public.quotes
),
gl AS (
  SELECT date, company,
         GREATEST(diff,0) AS gain,
         GREATEST(-diff,0) AS loss
  FROM ch WHERE diff IS NOT NULL
),
s AS (
  SELECT date, company,
         AVG(gain) OVER (PARTITION BY company ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS ag,
         AVG(loss) OVER (PARTITION BY company ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS al
  FROM gl
)
SELECT
  date, company,
  CASE WHEN al = 0 THEN 100 ELSE 100 - 100/(1 + ag/al) END AS rsi14
FROM s;
