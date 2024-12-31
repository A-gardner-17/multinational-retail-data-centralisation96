SELECT dim_date_times.year, dim_date_times.month, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales 
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC
LIMIT 10;