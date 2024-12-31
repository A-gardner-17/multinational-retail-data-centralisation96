-- Set timestamp column to a valid timestamp
UPDATE dim_date_times SET timestamp_column = CAST(
        year || '-' || LPAD(month, 2, '0') || '-' || LPAD(day, 2, '0') || ' ' || timestamp 
        AS TIMESTAMP);

-- Add new column to hold the interval value
ALTER TABLE dim_date_times ADD COLUMN difference INTERVAL;

-- Work out the interval and save to the new column
WITH calculated_differences AS (
            SELECT date_uuid, LEAD(timestamp_column) OVER (ORDER BY timestamp_column) - timestamp_column AS difference
        FROM 
            dim_date_times
        )
UPDATE dim_date_times
SET difference = calculated_differences.difference
FROM calculated_differences
WHERE dim_date_times.date_uuid = calculated_differences.date_uuid;

-- Calculate the yearly averages
SELECT EXTRACT(YEAR FROM timestamp_column) AS year, AVG(EXTRACT(EPOCH FROM difference)) AS average_difference_in_seconds
FROM dim_date_times
WHERE difference IS NOT NULL
GROUP BY EXTRACT(YEAR FROM timestamp_column)
ORDER BY average_difference_in_seconds DESC;