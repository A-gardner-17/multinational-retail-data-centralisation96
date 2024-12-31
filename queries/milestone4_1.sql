SELECT country_code, count (*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code;