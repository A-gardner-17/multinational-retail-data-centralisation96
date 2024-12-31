SELECT dim_store_details.store_type, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales,
        ROUND((SUM(orders_table.product_quantity * dim_products.product_price) * 100.0) / 
        SUM(SUM(orders_table.product_quantity * dim_products.product_price)) OVER (), 2) 
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY dim_store_details.store_type
ORDER BY total_sales DESC;