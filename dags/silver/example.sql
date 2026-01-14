CREATE OR REPLACE TABLE silver.example_table AS
SELECT
  1 AS id,
  'hola desde composer' AS message,
  CURRENT_TIMESTAMP() AS created_at;
