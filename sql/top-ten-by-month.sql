SELECT country_region,
       SUM(cases) AS monthly_cases
FROM covid19_processed
WHERE year = 2021 AND month(date) = 3
GROUP BY country_region
ORDER BY monthly_cases DESC
LIMIT 10;
