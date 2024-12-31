SELECT dim_store_details.country_code, count(dim_users.user_uuid) AS total_staff_number                   
FROM (SELECT DISTINCT country_code FROM dim_store_details) dim_store_details
JOIN dim_users ON dim_store_details.country_code = dim_users.country_code
GROUP BY dim_store_details.country_code
ORDER BY total_staff_number DESC;