
/*
PROJECT: Revenue Leakage Analysis (SQL)

GOAL:
This project simulates a real-world e-commerce database and performs
data ingestion + data quality validation to prepare for revenue leakage analysis.

Revenue leakage refers to money lost due to:
- Incorrect discounts
- Pricing mismatches
- Invalid relationships
- Data inconsistencies across orders, products, and customers

This script covers:
1. Database & table creation
2. Bulk CSV data loading
3. Bronze-layer validation checks
4. Referential integrity & anomaly detection

*/

create database revenue_leakage;
use revenue_leakage;

-- Orders table: stores order-level transaction details (billing summary)
create table orders (
order_id int primary key,
customer_id int,
order_time datetime,
payment_method varchar(50),
discount_pct decimal (5,2),
subtotal_usd decimal(10,2),
total_usd decimal (10,2),
country varchar (50),
device varchar (50),
source varchar (30));

-- Order items table: stores product-level line items for each order
create table order_items (
order_id int,
product_id int,
unit_price_usd decimal (10,2),
quantity int,
line_total_usd decimal (10,2));

-- Products table: master product catalog with pricing and cost information
create table products (
product_id	int primary key,
category varchar (50),
name varchar(100),	
price_usd decimal (10,2),
cost_usd decimal (10,2),	
margin_usd decimal (10,2)
);

-- Customers table: customer master data
create table customers (
customer_id int primary key,	
name varchar (100),	
email varchar(150),	
country varchar(50),	
age int,	
signup_date date,
marketing_opt_in boolean
);

---------------------------------------------------------------------------------------
-- SET GLOBAL local_infile = 1;
-- SHOW VARIABLES LIKE 'local_infile';
-- SHOW SESSION VARIABLES LIKE 'local_infile';
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Used to verify MySQL secure upload directory
SHOW VARIABLES LIKE 'secure_file_priv';
---------------------------------------------------------------------------------------

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv"
INTO TABLE customers
FIELDS TERMINATED BY ','        
ENCLOSED BY '"'                
LINES TERMINATED BY '\n'       
IGNORE 1 ROWS;                  

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/order_items.csv"
INTO TABLE order_items
FIELDS TERMINATED BY ','        
ENCLOSED BY '"'                
LINES TERMINATED BY '\n'       
IGNORE 1 ROWS;   

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/orders.csv"
INTO TABLE orders
FIELDS TERMINATED BY ','        
ENCLOSED BY '"'                
LINES TERMINATED BY '\n'       
IGNORE 1 ROWS;   

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv"
INTO TABLE products
FIELDS TERMINATED BY ','        
ENCLOSED BY '"'                
LINES TERMINATED BY '\n'       
IGNORE 1 ROWS;   

---------------------------------------------------------------
/*
BRONZE LAYER VALIDATION

Bronze tables represent raw ingested data.
At this stage:
- No transformations are applied
- Focus is on data quality checks
- Goal is to detect duplicates, missing references, and invalid values
*/
----------------------------------------------------------------
-- BRONZE LAYER DATA VALIDATION
-- (Raw data integrity checks)
---------------------------------------------------------------
select * from bronze_orders;
select * from bronze_order_items;
select * from bronze_products;
select * from bronze_customers;

-- check the duplocate order_id if exists
select 
	order_id,
	count(*) as orderid_cnt
from bronze_orders
group by order_id
having orderid_cnt > 1; -- no duplicate order_id exists

select 
	customer_id,
	count(*) as customerid_cnt
from bronze_customers
group by customer_id
having customerid_cnt> 1; -- no duplicate customer_id exists

select 
	product_id,
	count(*) as productid_cnt
from bronze_products
group by product_id
having productid_cnt> 1; -- no duplicate product_id exists

-- ORDERS WITHOUT CUSTOMER CHECK IF EXISTS
select distinct 
	o.order_id,
	c.customer_id
from bronze_orders as o
join bronze_customers as c
	on o.customer_id = c.customer_id
where c.customer_id is null;

-- THIS IS TO CHECK ORDER-ITEMS WITHOUT ORDERS IF ANY
select distinct 
	oi.order_id
from bronze_order_items as oi
left join bronze_orders as o
	on oi.order_id = o.order_id
where o.order_id is null;

-- TO CHECK ORDER-ITEMS WITHOUT ANY PRODUCT
select distinct 
	oi.product_id
from bronze_order_items as oi
left join bronze_products as p
	on oi.product_id= oi.product_id
where oi.product_id is null;

-- TO CHECK THE ORDERS WITHOUT CUSTOMERS
select distinct 
	o.customer_id
from bronze_orders as o
left join bronze_customers as c
	on o.customer_id = c.customer_id
where c.customer_id is null;

---------------------------------------
-- Critical null & value checks
--------------------------------------

-- TO MAKE SURE PRICES AND QUANTITIES MUST BE POSITIVE
select * from bronze_order_items
where quantity <= 0
or unit_price_usd <= 0
or line_total_usd <= 0;

-- CHECKING ORDERS WITH NEGATIVE OR ZERO TOTALS
select * from bronze_orders
where subtotal_usd <=0
or total_usd <= 0;

-- CHECKING THE DISCOUNTS
select * from bronze_orders
where discount_pct < 0
or discount_pct > 100;



-- =====================================================
-- Silver Layer: Order Items
-- Purpose: Validate line-level pricing integrity
-- =====================================================
create or replace view silver_order_items as
select
	order_id,
	product_id,
	unit_price_usd,
	quantity,
	line_total_usd,
unit_price_usd * quantity as expected_linetotal_usd ,  -- recalculating the line total to make sure it's correct
(unit_price_usd * quantity) - line_total_usd as line_total_diff_usd,
case
when abs((unit_price_usd * quantity) - line_total_usd) > 0.01 then "Price Mismatch"
else "Ok"
end as price_status
from bronze_order_items;

-- =====================================================
-- Silver Layer: Orders
-- Purpose: Validate discount and order total integrity
-- =====================================================
select * from bronze_orders;
create or replace view silver_orders as
select
	order_id,
	customer_id,
	order_time,
	payment_method,
	discount_pct,
	subtotal_usd,
	total_usd,
	country,
	device,
	source,
subtotal_usd * (1 - discount_pct/100 ) as expected_tota_usd,  -- expected total after applying the discount
(subtotal_usd * (1 - discount_pct/100 )) - total_usd as discount_diff_usd, -- difference between the total_usd and expected_usd
CASE
	WHEN discount_pct < 0 or discount_pct > 80 then "Invalid_Discount"
	WHEN ((subtotal_usd * (1 - discount_pct/100 )) - total_usd) > 0.01 then "Discount Mismatch"
	Else "Ok"
end as discount_status
from bronze_orders;

-- =====================================================
-- Silver Layer: Products
-- Purpose: Validate pricing and margin correctness
-- =====================================================
select * from bronze_products;
create or replace view silver_products as 
 select
	 product_id,
	 category,
	 name,
	 price_usd,
	 cost_usd,
	 margin_usd,
price_usd - cost_usd as expected_margin, -- to check if the data is correct
(price_usd - cost_usd) - margin_usd as margin_diff_usd, -- to check teh diff between the margin_usd and expected_margin
CASE
	WHEN cost_usd > price_usd THEN 'NEGATIVE_MARGIN'
	WHEN ABS((price_usd - cost_usd) - margin_usd) > 0.01 THEN 'MARGIN_MISMATCH'
	ELSE 'OK'
END AS margin_status
from bronze_products;

-- =====================================================
-- Silver Layer: Customers
-- Purpose: Clean and prepare customer master data
-- =====================================================

create or replace view silver_customers as 
select 
	customer_id,
	name,
	email,
	country,
	age,
	signup_date,
	marketing_opt_in
from bronze_customers;

-- =====================================================
-- Gold Layer: Revenue Leakage Summary
-- Purpose: At the company level, are we losing money between what we should have earned vs what we actually earned?
-- =====================================================
create or replace view gold_revenue_leakage_summary as
select 
current_date() as analysis_date,
round(sum(expected_linetotal_usd),2) as total_expected_revenue,
round(sum(line_total_usd),2) as total_realized_revenue,
round((sum(expected_linetotal_usd) - sum(line_total_usd)),2) as revenue_diff,
round((sum(expected_linetotal_usd) - sum(line_total_usd))/nullif(sum(line_total_usd),0)*100,2) as revenue_leakage_pct
from silver_order_items;

select * from gold_revenue_leakage_summary;

-- =====================================================
-- Purpose: Which products are responsible for the highest revenue leakage?
-- =====================================================

create or replace view gold_product_revenue_leakage as
select 
	p.product_id,
	p.name as product_name,
	p.category as product_category,
    
	sum(oi.quantity) as total_unit_sold,

	round(sum(expected_linetotal_usd),2) as expected_revenue,
	round(sum(line_total_usd),2) as realized_revenue,

	round((sum(expected_linetotal_usd)) - (sum(line_total_usd)),2) as revenue_leakage,

	round(((sum(expected_linetotal_usd)) - (sum(line_total_usd))) / nullif(sum(expected_linetotal_usd),0)*100,2) as revenue_leakage_pct

from silver_order_items as oi
join silver_products as p
on oi.product_id = p.product_id
group by 
	p.product_id,
	product_name,
	product_category;

select* from gold_product_revenue_leakage;

-- =====================================================
-- Gold Layer: Discount Abuse Analysis
-- Purpose: Which acquisition channels or sources are abusing or misapplying discounts?
-- =====================================================

create or replace view gold_discount_abuse_analysis as
select
	source,
    count(order_id) as total_orders,
	round(avg(discount_pct),2) as avg_discount_pct,
    round(sum(subtotal_usd * discount_pct / 100),2) as total_discount_value_usd,
    round(sum(discount_diff_usd),2) as discount_leakage_usd,
case
when avg(discount_pct) > 40
	 or sum(discount_diff_usd) > 1000
then "Yes" else "No"
end as discount_abuse_flag
from silver_orders
group by source;
select * from gold_discount_abuse_analysis;

-- =====================================================
-- Gold Layer: Customer Risk Profile
-- Purpose: Which customers generate revenue but pose a financial risk due to discount misuse?
-- =====================================================

create or replace view gold_customer_risk_profile as 
select 
	c.customer_id,
	c.country,
	count(order_id) as total_orders,
	round(sum(o.total_usd),2) as total_revenue,
	round(sum(o.subtotal_usd * discount_pct/100),2) as total_discount_received,
count(
	case
		when o.discount_status != "Ok" then o.order_id
    end) as discount_mismatch_count,
    
    round(sum(o.discount_diff_usd),2) as total_dis_leakage,
	round(sum(o.discount_diff_usd) / nullif(sum(o.total_usd), 0) * 100,2) AS leakage_pct_of_revenue,
    
    case
		when sum(o.discount_diff_usd) > 500
				and count(o.order_id) > 5
			then "High"
            when sum(o.discount_diff_usd) between 100 and 500
            then "Medium"
            else "Low"
	end as risk_category

from silver_orders o
join silver_customers c
    on o.customer_id = c.customer_id

group by
    c.customer_id,
    c.country;

 select *                                  -- CHECK
 from gold_customer_risk_profile
 where risk_category = "High"
 order by total_dis_leakage desc;

-- =====================================================
-- Gold Layer: Margin Performance
-- Purpose: Which products actually make money after accounting for cost?
-- =====================================================

CREATE OR REPLACE VIEW gold_margin_performance AS
SELECT
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,

    SUM(oi.quantity) AS total_units_sold,

    ROUND(SUM(oi.line_total_usd), 2) AS total_revenue,

    ROUND(SUM(p.cost_usd * oi.quantity), 2) AS total_cost,

    ROUND(
        SUM(oi.line_total_usd) - SUM(p.cost_usd * oi.quantity),
        2
    ) AS gross_profit,

    ROUND(
        (SUM(oi.line_total_usd) - SUM(p.cost_usd * oi.quantity))
        / NULLIF(SUM(oi.line_total_usd), 0) * 100,
        2
    ) AS gross_margin_pct

FROM silver_order_items oi
JOIN silver_products p
    ON oi.product_id = p.product_id

GROUP BY
    p.product_id,
    p.name,
    p.category;

select * from gold_margin_performance;

select * 
from gold_margin_performance
where gross_profit < 0; -- check fro negative marking

select 
	product_name,
	product_category,
	total_revenue,
	gross_margin_pct
from gold_margin_performance
where gross_margin_pct < 10
order by gross_margin_pct asc; -- to identify low-margin products

select
min(gross_margin_pct) as min_margin,
max(gross_margin_pct) as max_margin,
round(avg(gross_margin_pct),2) as avg_margin
from gold_margin_performance; -- margin disctrinution check table

 select 
 product_name,
 gross_profit
 from gold_margin_performance
 order by gross_profit desc
 limit 10; -- top profit contrinutors

SELECT
    ROUND(SUM(total_revenue), 2) AS gold_revenue,
    (
        SELECT ROUND(SUM(line_total_usd), 2)
        FROM silver_order_items
    ) AS silver_revenue
FROM gold_margin_performance;

-- =====================================================
-- Gold Layer (Advanced): Customer Leakage Ranking
-- Purpose:
-- Rank customers by revenue leakage using window functions
-- to identify top-risk customers relative to peers
-- =====================================================

create or replace view gold_customer_leakage_ranked  as
with customer_leakage as(
select 
	c.customer_id,
	c.country,

	count(o.order_id) as total_orders,
	round(sum(o.total_usd),2) as total_revenue,
	round(sum(o.discount_diff_usd),2) as total_leakage

from silver_orders as o
join silver_customers as c
	on o.customer_id = c.customer_id
group by 
o.customer_id,
c.country )

select 
	customer_id,
	country,
	total_orders,
	total_revenue,
	total_leakage,

    rank() over(partition by country order by total_leakage desc) as leakage_rank_in_country, -- Rank customers by leakage within each country
    round(percent_rank() over(partition by country order by total_leakage desc) * 100,2) as leakage_percentile, -- Percentile ranking for relative risk
    
case    
when percent_rank() over(partition by country order by total_leakage desc) >= 0.90 then "High"  -- Risk categorization using percentile logic
when percent_rank() over(partition by country order by total_leakage desc) >= 0.70 then "Medium"
else "Low"
end as risk_category

from customer_leakage;

--------------------------------------------------------------------
-- Analysis we can perform on gold_customer_leakage_ranked
--------------------------------------------------------------------
select * from gold_customer_leakage_ranked;
select * from gold_customer_leakage_ranked
where risk_category = "High";

select -- -- Check how leakage risk is distributed by country
	country,
	risk_category,
	count(*) as customer_count
from gold_customer_leakage_ranked
group by country, risk_category
order by country, risk_category;


select  -- High revenue but high risk
	customer_id,
	total_revenue,
	total_leakage,
	leakage_percentile,
	risk_category
from gold_customer_leakage_ranked
where risk_category = "High" and
total_revenue > (select avg(total_revenue)
					from gold_customer_leakage_ranked)
order by total_leakage desc;

select
min(leakage_percentile) as min_percentile,
max(leakage_percentile) as max_percentile
from gold_customer_leakage_ranked;
