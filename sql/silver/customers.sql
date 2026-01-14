CREATE OR REPLACE TABLE silver.customers AS
SELECT
  customer_id,
  name,
  COALESCE(city, 'UNKNOWN') AS city,
  DATE(signup_date) AS signup_date
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date) AS rn
  FROM bronze.customers_raw
)
WHERE rn = 1;
