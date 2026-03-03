SELECT 
    year,
    month(date) AS month,
    SUM(cases) AS monthly_cases
FROM covid19_processed
GROUP BY year, month(date)
ORDER BY year, month(date);