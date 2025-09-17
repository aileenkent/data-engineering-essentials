# 10 Common SQL Data Transformations with Practice Datasets

## Practice Datasets

Before diving into the transformations, here are reliable sources for practice datasets:

### Recommended Practice:
- **Northwind Database**: Classic e-commerce dataset with customers, orders, products
  - Use the SQL scripts in `northwind/` to create and populate the DB.
  - Download: https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs
  - CSV Files: https://www.datablist.com/learn/csv/download-sample-csv-files (search "Northwind")

- **Sakila Database**: DVD rental store dataset with customers, films, rentals
  - Download: https://www.kaggle.com/datasets/atanaskanev/sqlite-sakila-sample-database
  - Original: https://dev.mysql.com/doc/sakila/en/

- **Sample CSV Files**: Various business datasets
  - Source: https://www.datablist.com/learn/csv/download-sample-csv-files
  - GitHub: https://github.com/datablist/sample-csv-files

- **Kaggle SQL Datasets**: Real-world data for practice
  - Browse: https://www.kaggle.com/datasets?tags=16641-SQL
  - Olympics Dataset: https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results

---

## 1. Filtering with WHERE clauses
Selecting specific rows based on conditions to focus on relevant data subsets.

**Practice with:** Northwind `Products` and `Customers` tables

```sql
-- Using Northwind database
-- Filter products
SELECT product_id, product_name, unit_price, units_in_stock
FROM products 
WHERE unit_price >= 20
  AND unit_price < 100
  AND units_in_stock != 0;

-- Filter customers from specific countries
SELECT customer_id, company_name, country, city
FROM customers 
WHERE country IN ('USA', 'Germany', 'France') 
  AND city IS NOT NULL;
```

## 2. Aggregating with GROUP BY
Summarizing data using aggregate functions to calculate metrics at different granularity levels.

**Practice with:** Northwind `Order Details` and `Products` tables

```sql
-- Total sales by product category (Northwind)
SELECT c.category_name, 
       COUNT(DISTINCT od.order_id) as order_count,
       SUM(od.unit_price * od.quantity * (1 - od.discount)) as total_sales,
       AVG(od.unit_price * od.quantity * (1 - od.discount)) as avg_order_value
FROM order_details od
JOIN products p ON od.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
GROUP BY c.category_name;

-- Monthly sales trends (Northwind)
SELECT YEAR(o.order_date) as year,
       MONTH(o.order_date) as month,
       SUM(od.unit_price * od.quantity * (1 - od.discount)) as monthly_revenue
FROM orders o
JOIN order_details od ON o.order_id = od.order_id
GROUP BY YEAR(o.order_date), MONTH(o.order_date)
ORDER BY year, month;
```

## 3. Joining tables
Combining data from multiple tables to create comprehensive datasets.

**Practice with:** Sakila `customer`, `rental`, `film` tables or Northwind tables

```sql
-- Customer orders with product details (Northwind)
SELECT c.company_name, 
       o.order_date, 
       p.product_name, 
       od.quantity,
       od.unit_price * od.quantity * (1 - od.discount) as line_total
FROM customers c
INNER JOIN Orders o ON c.customer_id = o.customer_id
INNER JOIN order_details od ON o.order_id = od.order_id
INNER JOIN products p ON od.product_id = p.product_id;

-- All customers with their rental history (Sakila)
-- SELECT c.first_name, c.last_name, 
--        COALESCE(rental_count, 0) as total_rentals
-- FROM customer c
-- LEFT JOIN (
--     SELECT customer_id, COUNT(*) as rental_count
--     FROM rental 
--     GROUP BY customer_id
-- ) r ON c.customer_id = r.customer_id;
```

## 4. Creating calculated fields
Using arithmetic operations and functions to derive new columns.

**Practice with:** Northwind `Products` table or sample e-commerce CSV

```sql
-- Calculate profit margins and price categories (Northwind)
SELECT product_name,
       unit_price,
       units_in_stock,
       units_in_stock * unit_price as inventory_value,
       CASE 
           WHEN unit_price > 50 THEN 'Premium'
           WHEN unit_price > 20 THEN 'Standard'
           ELSE 'Budget'
       END as price_category,
       CASE 
           WHEN units_in_stock = 0 THEN 'Out of Stock'
           WHEN units_in_stock < 10 THEN 'Low Stock'
           ELSE 'In Stock'
       END as stock_status
FROM products;

-- Create customer segments from order history
SELECT c.customer_id,
       c.company_name,
       COUNT(o.order_id) as order_count,
       SUM(od.unit_price * od.quantity) as total_spent,
       CASE 
           WHEN COUNT(o.order_id) > 10 AND SUM(od.unit_price * od.quantity) > 2000 THEN 'VIP'
           WHEN SUM(od.unit_price * od.quantity) > 1000 THEN 'High Value'
           WHEN COUNT(o.order_id) > 5 THEN 'Regular'
           ELSE 'New'
       END as customer_segment
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_details od ON o.order_id = od.order_id
GROUP BY c.customer_id, c.company_name;
```

## 4a. Creating Calculated Fields - DOB

Imagine a table that provides `birth_date` in some format, such as `YYYY-MM-DD`.
To introduce a new calculated column that derives an age (in years) it must be 
calculated in realtime.

DuckDB has two useful built-in functions for this calculation: `date_diff` and
strong DateTime features to extract `year` from a `datetime` value.

```
ALTER TABLE your_table_name 
ADD COLUMN age INTEGER;

UPDATE your_table_name 
SET age = date_diff('year', birth_date, CURRENT_DATE);
```

## 5. Window functions
Performing calculations across related rows using OVER() clauses.

**Practice with:** Northwind `Orders` table or time-series CSV data

```sql
-- Running total and ranking (Northwind)
SELECT o.customer_id,
       o.order_date,
       od.unit_price * od.quantity as order_value,
       SUM(od.unit_price * od.quantity) OVER (
           PARTITION BY o.customer_id 
           ORDER BY o.order_date
       ) as running_total,
       ROW_NUMBER() OVER (
           PARTITION BY o.customer_id 
           ORDER BY od.unit_price * od.quantity DESC
       ) as order_rank,
       AVG(od.unit_price * od.quantity) OVER (
           PARTITION BY o.customer_id
       ) as customer_avg_order
FROM orders o
JOIN order_details od ON o.order_id = od.order_id;
```

## 6. Date and time manipulation
Extracting components and performing date calculations for temporal analysis.

**Practice with:** Any dataset with date columns (Orders, Rentals, etc.)

```sql
-- Extract date components for analysis (Northwind)

SELECT order_id,
       order_date,
       YEAR(order_date) as year,
       MONTH(order_date) as month,
       DAYOFWEEK(order_date) as day_of_week,
       CASE DAYOFWEEK(order_date)
           WHEN 1 THEN 'Monday'
           WHEN 2 THEN 'Tuesday'
           WHEN 3 THEN 'Wednesday'
           WHEN 4 THEN 'Thursday'
           WHEN 5 THEN 'Friday'
           WHEN 6 THEN 'Saturday'
           WHEN 7 THEN 'Sunday'
       END as day_name,
       (CURRENT_DATE - order_date) as days_since_order
FROM orders;
```

## 7. Pivoting and unpivoting
Reshaping data by converting rows to columns or vice versa.

**Practice with:** Sales data by time periods or categories

```sql
-- Pivot: Convert monthly sales rows to columns (Northwind)
SELECT p.category_id,
       c.category_name,
       SUM(CASE WHEN MONTH(o.OrderDate) = 1 THEN od.UnitPrice * od.quantity ELSE 0 END) as Jan_Sales,
       SUM(CASE WHEN MONTH(o.OrderDate) = 2 THEN od.UnitPrice * od.Quantity ELSE 0 END) as Feb_Sales,
       SUM(CASE WHEN MONTH(o.OrderDate) = 3 THEN od.UnitPrice * od.Quantity ELSE 0 END) as Mar_Sales,
       SUM(CASE WHEN MONTH(o.OrderDate) = 4 THEN od.UnitPrice * od.Quantity ELSE 0 END) as Apr_Sales
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN [Order Details] od ON p.ProductID = od.ProductID
JOIN Orders o ON od.OrderID = o.OrderID
WHERE YEAR(o.OrderDate) = 1997
GROUP BY p.CategoryID, c.CategoryName;

-- Unpivot: Convert quarterly columns to rows
-- (Use with sample data that has Q1_Sales, Q2_Sales, etc. columns)
SELECT customer_id, 'Q1' as quarter, q1_sales as sales FROM quarterly_summary
UNION ALL
SELECT customer_id, 'Q2' as quarter, q2_sales as sales FROM quarterly_summary
UNION ALL
SELECT customer_id, 'Q3' as quarter, q3_sales as sales FROM quarterly_summary
UNION ALL
SELECT customer_id, 'Q4' as quarter, q4_sales as sales FROM quarterly_summary;
```

## 8. Handling missing values
Using functions to identify and replace NULL values.

**Practice with:** Customer data or any dataset with optional fields

```sql
-- Replace NULL values with defaults (Northwind)
SELECT customer_id,
       company_name,
       COALESCE(contact_name, 'No Contact') as contact_name,
       COALESCE(phone, 'Not Provided') as phone,
       COALESCE(fax, 'No Fax') as fax_number,
       CASE 
           WHEN address IS NULL THEN 'Address Missing'
           ELSE address
       END as clean_address
FROM customers;

-- Identify patterns in missing data
SELECT 
    COUNT(*) as total_customers,
    COUNT(contact_name) as contact_name_count,
    COUNT(*) - COUNT(contact_name) as contact_missing,
    ROUND((COUNT(*) - COUNT(contact_name)) * 100.0 / COUNT(*), 2) as contact_missing_pct,
    COUNT(phone) as phone_count,
    ROUND(COUNT(phone) * 100.0 / COUNT(*), 2) as phone_completion_pct
FROM customers;
```

## 9. String manipulation
Cleaning and standardizing text data for consistent formatting.

**Practice with:** Customer or product name data

```sql
-- Clean and standardize text fields (Northwind)
SELECT product_id,
       UPPER(TRIM(product_name)) as clean_product_name,
       CONCAT(TRIM(product_name), ' - ', category_id) as product_with_category,
       LENGTH(product_name) as name_length,
       CASE 
           WHEN product_name LIKE '%Chocolate%' THEN 'Chocolate'
           WHEN product_name LIKE '%Cheese%' THEN 'Cheese'
           WHEN product_name LIKE '%Coffee%' THEN 'Beverage'
           ELSE 'Other'
       END as product_type
FROM Products;

-- Extract information from customer data
SELECT CustomerID,
       CompanyName,
       SUBSTRING(CustomerID, 1, 2) as country_code,
       CASE 
           WHEN CompanyName LIKE '%Ltd%' OR CompanyName LIKE '%Limited%' THEN 'Limited Company'
           WHEN CompanyName LIKE '%Inc%' OR CompanyName LIKE '%Corp%' THEN 'Corporation'
           WHEN CompanyName LIKE '%LLC%' THEN 'LLC'
           ELSE 'Other'
       END as company_type,
       REPLACE(REPLACE(COALESCE(Phone, ''), '(', ''), ')', '') as clean_phone
FROM Customers;
```

## 10. Subqueries and CTEs (Common Table Expressions)
Breaking complex queries into manageable parts for cleaner, more readable code.

**Practice with:** Multi-table analysis requiring complex logic

```sql
-- Using CTE for multi-step customer analysis (Northwind)
WITH customer_metrics AS (
    SELECT c.CustomerID,
           c.CompanyName,
           COUNT(DISTINCT o.OrderID) as order_count,
           SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) as total_spent,
           AVG(od.UnitPrice * od.Quantity * (1 - od.Discount)) as avg_order_value,
           MAX(o.OrderDate) as last_order_date
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    LEFT JOIN [Order Details] od ON o.OrderID = od.OrderID
    GROUP BY c.CustomerID, c.CompanyName
),
customer_segments AS (
    SELECT CustomerID,
           CompanyName,
           order_count,
           total_spent,
           avg_order_value,
           CASE 
               WHEN total_spent > 5000 AND order_count > 10 THEN 'VIP'
               WHEN total_spent > 2000 THEN 'High Value'
               WHEN order_count > 5 THEN 'Frequent'
               ELSE 'Standard'
           END as segment,
           DATEDIFF(CURDATE(), last_order_date) as days_since_last_order
    FROM customer_metrics
)
SELECT segment,
       COUNT(*) as customer_count,
       AVG(total_spent) as avg_lifetime_value,
       AVG(days_since_last_order) as avg_days_since_last_order
FROM customer_segments
GROUP BY segment
ORDER BY avg_lifetime_value DESC;

-- Subquery: Find products with above-average unit prices in their category
SELECT p1.ProductName, p1.CategoryID, p1.UnitPrice
FROM Products p1
WHERE p1.UnitPrice > (
    SELECT AVG(p2.UnitPrice) 
    FROM Products p2 
    WHERE p2.CategoryID = p1.CategoryID
)
ORDER BY p1.CategoryID, p1.UnitPrice DESC;
```

## Getting Started with Practice

1. **Download sample databases:**
   - Sakila: https://www.kaggle.com/datasets/atanaskanev/sqlite-sakila-sample-database
   - Northwind: Available from Microsoft's sample databases repository
   - CSV files: https://www.datablist.com/learn/csv/download-sample-csv-files

2. **Practice progression:**
   - Start with simple SELECT and WHERE clauses
   - Progress to JOINs and aggregations
   - Master window functions and CTEs
   - Combine multiple techniques in complex queries
