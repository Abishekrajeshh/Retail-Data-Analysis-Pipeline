CREATE TABLE df_orders (
    order_id INT PRIMARY KEY,
    order_date DATE,
    ship_mode VARCHAR(20),
    segment VARCHAR(20),
    country VARCHAR(20),
    city VARCHAR(20),
    state VARCHAR(20),
    postal_code VARCHAR(20),
    region VARCHAR(20),
    category VARCHAR(20),
    sub_category VARCHAR(20),
    product_id VARCHAR(50),
    quantity INT,
    discount DECIMAL(7,2),
    sale_price DECIMAL(7,2),
    profit DECIMAL(7,2)
);



SELECT * FROM df_orders


--- FIND TOP 10 HIGHEST REVENUE GENERATED PRODUCTS

SELECT 
    product_id, 
    category, 
    SUM(profit) AS total_profit, 
    SUM(sale_price) AS total_sales
FROM df_orders
GROUP BY product_id, category
ORDER BY total_sales DESC
LIMIT 10;


--- FIND TOP 5 HIGHEST SELLING PRODUCTS IN EACH REGION

WITH ranked_sales AS (
    SELECT 
        region,
        product_id,
        SUM(sale_price) AS sales,
        ROW_NUMBER() OVER (
            PARTITION BY region 
            ORDER BY SUM(sale_price) DESC
        ) AS rn
    FROM df_orders
    GROUP BY region, product_id
)
SELECT 
    region,
    product_id,
    sales,
	rn
FROM ranked_sales
WHERE rn <= 5
ORDER BY region, sales DESC;


--Find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023

WITH cte AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        SUM(sale_price) AS sales
    FROM df_orders
    WHERE EXTRACT(YEAR FROM order_date) IN (2022, 2023)
    GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
)
SELECT 
    order_month,
    ROUND(SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END)::numeric, 2) AS sales_2022,
    ROUND(SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END)::numeric, 2) AS sales_2023
FROM cte
GROUP BY order_month
ORDER BY order_month;

--For each category which month had highest sales 

WITH cte AS (
    SELECT 
        category,
        TO_CHAR(order_date, 'YYYYMM') AS order_year_month,
        SUM(sale_price) AS sales 
    FROM df_orders
    GROUP BY category, TO_CHAR(order_date, 'YYYYMM')
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
    FROM cte
) a
WHERE rn = 1;


--Which sub category had highest growth by profit in 2023 compare to 2022


WITH cte AS (
    SELECT 
        sub_category,
        EXTRACT(YEAR FROM order_date) AS order_year,
        SUM(profit) AS total_profit
    FROM df_orders
    GROUP BY sub_category, EXTRACT(YEAR FROM order_date)
),
cte2 AS (
    SELECT 
        sub_category,
        ROUND(SUM(CASE WHEN order_year = 2022 THEN total_profit ELSE 0 END):: numeric, 2) AS profit_2022,
        ROUND(SUM(CASE WHEN order_year = 2023 THEN total_profit ELSE 0 END):: numeric, 2) AS profit_2023
    FROM cte 
    GROUP BY sub_category
)
SELECT *,
       (profit_2023 - profit_2022) AS profit_growth
FROM cte2
ORDER BY profit_growth DESC
LIMIT 1;






