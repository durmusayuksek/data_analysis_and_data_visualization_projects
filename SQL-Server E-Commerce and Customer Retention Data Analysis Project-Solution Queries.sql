-- E-Commerce Data and Customer Retention Analysis with SQL - DAwSQL Project

-- Part-1 : Analyze the data by finding the answers to the questions below:

-- 1. Join all the tables and create a new table with all of the columns, called combined_table. 
-- (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen) 

SELECT c.Cust_id, c.Customer_Name, c.Province, c.Region, c.Customer_Segment, o.Ord_id, o.Order_Date, o.Order_Priority,
s.Ship_id, s.Order_ID, s.Ship_Date, s.Ship_Mode, p.Prod_id, p.Product_Category, p.Product_Sub_Category, 
m.Sales, m.Discount, m.Order_Quantity, m.Product_Base_Margin
INTO combined_table
FROM cust_dimen c JOIN market_fact m ON c.cust_id=m.cust_id JOIN orders_dimen o ON m.ord_id=o.ord_id 
JOIN shipping_dimen s ON s.ship_id=m.ship_id JOIN prod_dimen p ON p.prod_id=m.prod_id

-- 2. Find the top 3 customers who have the maximum count of orders.

SELECT c.Cust_id, c.Customer_Name, T1.order_count
FROM cust_dimen c JOIN
            (SELECT TOP 3 cust_id, count(ord_id) AS order_count
            FROM combined_table
            GROUP BY Cust_id
            ORDER BY order_count DESC) as T1 ON c.cust_id=T1.cust_id
ORDER BY T1.order_count DESC;

-- 3. Create a new column at combined_table as DaysTakenForDelivery that 
-- contains the date difference of Order_Date and Ship_Date.

ALTER TABLE combined_table ADD DaysTakenForDelivery INTEGER;

UPDATE combined_table
SET daystakenfordelivery = DATEDIFF(DAY, order_date, ship_date)

SELECT TOP 100 *
FROM combined_table

-- 4. Find the customer whose order took the maximum time to get delivered.

SELECT DISTINCT TOP 1 cust_id, customer_name, order_date, ship_date, daystakenfordelivery 
FROM combined_table
ORDER BY daystakenfordelivery DESC

-- 5. Count the total number of unique customers in January and how many of them 
-- came back every month over the entire year in 2011.

SELECT MONTH(Order_Date) AS month, COUNT(DISTINCT Cust_id) AS customer_count
FROM combined_table
WHERE Cust_id in
            (SELECT DISTINCT Cust_id AS customers
            FROM combined_table
            WHERE YEAR(Order_Date) = 2011 AND MONTH(Order_Date) = 1) and YEAR(Order_Date) = 2011
GROUP BY MONTH(Order_Date)

-- 6. Write a query to return for each user the time elapsed between the first 
-- purchasing and the third purchasing, in ascending order by Customer ID.

WITH new_table 
AS (
    SELECT DISTINCT cust_id, order_date
    FROM combined_table
)
SELECT DISTINCT t4.cust_id, t4.first_order, t4.third_order,
DATEDIFF(DAY, (SELECT MIN(order_date) FROM combined_table), t4.third_order ) - 
DATEDIFF(DAY, (SELECT MIN(order_date) FROM combined_table), t4.first_order)
FROM
    (SELECT t3.cust_id, 
    CASE WHEN t3.birinci IS NOT NULL THEN t3.birinci ELSE LAG(t3.birinci) 
        OVER(PARTITION BY cust_id ORDER BY cust_id) END AS first_order,
    CASE WHEN t3.ucuncu IS NOT NULL THEN t3.ucuncu ELSE LEAD(t3.ucuncu) 
        OVER(PARTITION BY cust_id ORDER BY t3.ucuncu) END AS third_order
    FROM(
        SELECT t2.cust_id, t2.birinci, t2.ucuncu 
        FROM
            (SELECT cust_id, order_date,
            ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY order_date) AS third,
            CASE WHEN COUNT(order_date) OVER(PARTITION BY cust_id) >= 3 AND
            ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY order_date) = 1 THEN order_date ELSE NULL END AS birinci,
            CASE WHEN COUNT(order_date) OVER(PARTITION BY cust_id) >= 3 AND
            ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY order_date) = 3 THEN order_date ELSE NULL END AS ucuncu
            FROM new_table) AS t2
    WHERE t2.birinci IS NOT NULL OR t2.ucuncu IS NOT NULL) AS t3) AS t4
ORDER BY t4.cust_id

-- 7. Write a query that returns customers who purchased both product 11 and product 14 as well as the ratio of 
-- these products to the total number of products purchased by the customer.

SELECT cust_id, 
SUM(CASE WHEN prod_id = 11 THEN order_quantity ELSE 0 END) AS P11,
SUM(CASE WHEN prod_id = 14 THEN order_quantity ELSE 0 END) AS P14,
SUM(Order_Quantity) AS total_products,
ROUND(CAST(SUM(CASE WHEN prod_id = 11 THEN order_quantity ELSE 0 END) AS FLOAT) / SUM(Order_Quantity), 2) AS Ratio_P11,
ROUND(CAST(SUM(CASE WHEN prod_id = 14 THEN order_quantity ELSE 0 END) AS FLOAT) / SUM(Order_Quantity), 2) AS Ratio_P14
    FROM combined_table 
    WHERE cust_id IN
        (SELECT cust_id
        FROM combined_table 
        WHERE prod_id=11
        INTERSECT
        SELECT cust_id
        FROM combined_table 
        WHERE prod_id=14)
    GROUP BY cust_id

-- Part-2: Customer Segmentation
-- Categorize customers based on their frequency of visits.

-- 1. Create a view that keeps visit logs of customers on a monthly basis.
-- (For each log, three field is kept: Cust_id, Year, Month)

CREATE VIEW customer_logs AS 
SELECT cust_id, YEAR(order_date) AS year, MONTH(order_date) AS month 
FROM combined_table

SELECT * FROM customer_logs 
ORDER BY cust_id, year, month

-- 2. Create a view that keeps the number of monthly visits by users.
-- (Separately for all months from the business beginning)

CREATE VIEW monthly_visits AS
SELECT cust_id, YEAR(order_date) AS year, MONTH(order_date) AS month, COUNT(order_date) AS num_of_logs 
FROM combined_table 
GROUP BY cust_id, year(order_date), month(order_date)

SELECT * FROM monthly_visits
ORDER BY cust_id, year, month

-- 3. For each visit of customers, create the month of the visit as a separate column.

CREATE VIEW monthly_cust_table AS
(SELECT t1.cust_id, t1.year, t1.month, t1.num_of_logs, t1.current_month, t1.next_visit_month  
FROM
    (SELECT DISTINCT cust_id, YEAR(order_date) as year, MONTH(order_date) AS month, 
    COUNT(order_date) OVER(PARTITION BY cust_id, YEAR(order_date), MONTH(order_date)) AS num_of_logs,
    DATEDIFF(MONTH, (SELECT MIN(order_date) FROM combined_table), order_date) + 1 AS current_month,
    DATEDIFF(MONTH, (SELECT MIN(order_date) FROM combined_table), LEAD(order_date, 1) OVER (PARTITION BY cust_id 
        ORDER BY order_date)) + 1 AS next_visit_month
    FROM combined_table) AS t1 
EXCEPT 
SELECT t1.cust_id, t1.year, t1.month, t1.num_of_logs, t1.current_month, t1.next_visit_month 
FROM 
    (SELECT DISTINCT cust_id, YEAR(order_date) as year, MONTH(order_date) AS month, 
    COUNT(order_date) OVER(PARTITION BY cust_id, YEAR(order_date), MONTH(order_date)) AS num_of_logs,
    DATEDIFF(MONTH, (SELECT MIN(order_date) FROM combined_table), order_date) + 1 AS current_month,
    DATEDIFF(MONTH, (SELECT MIN(order_date) FROM combined_table), LEAD(order_date, 1) OVER (PARTITION BY cust_id 
        ORDER BY order_date)) + 1 AS next_visit_month
    FROM combined_table) AS t1  
WHERE t1.current_month = t1.next_visit_month)

SELECT * FROM monthly_cust_table

-- 4. Calculate the monthly time gap between two consecutive visits by each customer.

SELECT cust_id, year, month, num_of_logs, current_month, next_visit_month,
next_visit_month - current_month AS time_gaps
FROM monthly_cust_table

-- 5. Categorize customers using average time gaps. Choose the most fitted labeling model for you.
-- For example:
---Labeled as churn if the customer hasn't made another purchase in the months since they made their first purchase.
---Labeled as regular if the customer has made a purchase every month. Etc.

SELECT cust_id, AVG(next_visit_month - current_month) AS avg_time_gap,
CASE WHEN AVG(next_visit_month - current_month) IS NULL THEN 'Churn' ELSE 'Irregular' END AS cust_labels
FROM monthly_cust_table
GROUP BY cust_id

-- Part-3: Month-Wise Retention Rate
-- 1. Find month-by-month customer retention rate since the start of the business.

SELECT cust_id, year, month, current_month, next_visit_month,
next_visit_month - current_month AS time_gap, 
COUNT(cust_id) OVER(PARTITION BY year, month) AS retention_month_wise
FROM monthly_cust_table
WHERE next_visit_month - current_month = 1
ORDER BY month, year

-- 2. Calculate the month-wise retention rate.

SELECT year, month,
ROUND(CAST(SUM(CASE WHEN next_visit_month - current_month = 1 THEN 1 ELSE 0 END) AS FLOAT) / lag(COUNT(cust_id), 1) OVER(ORDER BY year, month), 2) AS retention_rate
FROM monthly_cust_table
GROUP BY year, month
EXCEPT
SELECT year, month,
ROUND(CAST(SUM(CASE WHEN next_visit_month - current_month = 1 THEN 1 ELSE 0 END) AS FLOAT) / lag(COUNT(cust_id), 1) OVER(ORDER BY year, month), 2) AS retention_rate
FROM monthly_cust_table
WHERE (YEAR = (SELECT YEAR(MIN(order_date)) FROM combined_table) AND month = (SELECT MONTH(MIN(order_date)) FROM combined_table)) 
OR (YEAR = (SELECT YEAR(MAX(order_date)) FROM combined_table) AND month = (SELECT MONTH(MAX(order_date)) FROM combined_table))
GROUP BY year, month
ORDER BY year, month