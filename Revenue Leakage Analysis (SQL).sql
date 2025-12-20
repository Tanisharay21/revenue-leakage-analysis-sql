
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



