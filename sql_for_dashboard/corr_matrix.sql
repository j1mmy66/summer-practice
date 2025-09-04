WITH r AS (
  SELECT date, company,
         close / LAG(close) OVER (PARTITION BY company ORDER BY date) - 1 AS ret
  FROM public.quotes
),
rr AS (SELECT * FROM r WHERE ret IS NOT NULL)
SELECT a.company AS x, b.company AS y, CORR(a.ret, b.ret) AS corr
FROM rr a
JOIN rr b ON a.date = b.date
GROUP BY 1,2
ORDER BY 1,2;
