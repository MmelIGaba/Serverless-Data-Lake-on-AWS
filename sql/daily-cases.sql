SELECT date,
       SUM(cases) AS daily_cases
FROM covid19_processed
WHERE country_region = 'South Africa'
GROUP BY date
ORDER BY date;