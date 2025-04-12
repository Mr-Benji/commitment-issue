SELECT
  transaction_id,
  product_name,
  branch_name,
  sale_amount,
  sale_date,
  LAG(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) AS previous_amount,
  LEAD(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) AS next_amount,
  CASE
    WHEN LAG(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) IS NULL THEN 'N/A'
    WHEN sale_amount > LAG(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) THEN 'HIGHER'
    WHEN sale_amount < LAG(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) THEN 'LOWER'
    WHEN sale_amount = LAG(sale_amount) OVER (PARTITION BY branch_name ORDER BY sale_date) THEN 'EQUAL'
    ELSE 'N/A'
  END AS comparison_with_previous
FROM
  sales_transactions
ORDER BY
  branch_name, sale_date;


-- RANK() and DENSE_RANK() within category
SELECT
  transaction_id,
  product_name,
  category,
  sale_amount,
  RANK() OVER (PARTITION BY category ORDER BY sale_amount DESC) AS rank_sale,
  DENSE_RANK() OVER (PARTITION BY category ORDER BY sale_amount DESC) AS dense_rank_sale
FROM
  sales_transactions
ORDER BY
  category, sale_amount DESC;

<--- Identifying Top Records--->

WITH ranked_sales AS (
  SELECT
    transaction_id,
    product_name,
    category,
    sale_amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sale_amount DESC) AS rn
  FROM
    sales_transactions
)
SELECT
  transaction_id,
  product_name,
  category,
  sale_amount
FROM
  ranked_sales
WHERE
  rn <= 3
ORDER BY
  category, sale_amount DESC;

  <----Finding the Earliest Records.----->

  WITH ranked_sales AS (
  SELECT
    transaction_id,
    product_name,
    category,
    sale_date,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sale_date ASC) AS rn
  FROM
    sales_transactions
)
SELECT
  transaction_id,
  product_name,
  category,
  sale_date
FROM
  ranked_sales
WHERE
  rn <= 2
ORDER BY
  category, sale_date;

<---Aggregation with Window Functions!--->
SELECT
  transaction_id,
  product_name,
  category,
  sale_amount,
  MAX(sale_amount) OVER (PARTITION BY category) AS max_in_category,
  MAX(sale_amount) OVER () AS overall_max
FROM
  sales_transactions
ORDER BY
  category, sale_amount DESC;

