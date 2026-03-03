SELECT country_region,
       SUM(cases) AS total_cases
FROM covid19_processed
GROUP BY country_region
ORDER BY total_cases DESC
LIMIT 10;
