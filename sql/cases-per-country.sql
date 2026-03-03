SELECT country_region,
       year,
       SUM(cases) AS total_cases
FROM covid19_processed
GROUP BY country_region, year
ORDER BY total_cases DESC;
