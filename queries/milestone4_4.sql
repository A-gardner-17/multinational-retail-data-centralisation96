SELECT 
CASE 
    WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
    ELSE 'Offline'
END AS location,
COUNT(orders_table.index) AS number_of_sales,
SUM(orders_table.product_quantity) AS product_quantity_count
FROM orders_table
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY location;