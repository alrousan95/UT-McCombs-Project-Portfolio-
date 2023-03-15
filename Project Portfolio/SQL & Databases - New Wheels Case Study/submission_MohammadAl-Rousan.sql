/*

-----------------------------------------------------------------------------------------------------------------------------------
                                               Guidelines
-----------------------------------------------------------------------------------------------------------------------------------

The provided document is a guide for the project. Follow the instructions and take the necessary steps to finish
the project in the SQL file			
-----------------------------------------------------------------------------------------------------------------------------------

											Database Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------
*/

-- [1] To begin with the project, you need to create the database first
-- Write the Query below to create a Database
create database Vehdb;

-- [2] Now, after creating the database, you need to tell MYSQL which database is to be used.
-- Write the Query below to call your Database
use Vehdb;

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Tables Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [3] Creating the tables:

 create table temp_t(
	shipper_id int,
	shipper_name varchar(50),
	shipper_contact_details varchar(30),
	product_id int,
	vehicle_maker varchar(60),
	vehicle_model varchar(60),
	vehicle_color varchar(60),
	vehicle_model_year int,
	vehicle_price decimal(14,2),
	quantity int,
	discount decimal(4,2),
	customer_id varchar(25),
	customer_name varchar(25),
	gender varchar(15),
	job_title varchar(50),
	phone_number varchar(20),
	email_address varchar(50),
	city varchar(25),
	country varchar(40),
	state varchar(40),
	customer_address varchar(50),
	order_date date,
	order_id varchar(25),
	ship_date date,
	ship_mode varchar(25),
	shipping varchar(30),
	postal_code int,
	credit_card_type varchar(40),
	credit_card_number bigint,
	customer_feedback varchar(20),
	quarter_number int,
    primary key (shipper_id, order_id, product_id, customer_id)
);

create table vehicles_t(
	shipper_id int,
	shipper_name varchar(50),
	shipper_contact_details varchar(30),
	product_id int,
	vehicle_maker varchar(60),
	vehicle_model varchar(60),
	vehicle_color varchar(60),
	vehicle_model_year int,
	vehicle_price decimal(14,2),
	quantity int,
	discount decimal(4,2),
	customer_id varchar(25),
	customer_name varchar(25),
	gender varchar(15),
	job_title varchar(50),
	phone_number varchar(20),
	email_address varchar(50),
	city varchar(25),
	country varchar(40),
	state varchar(40),
	customer_address varchar(50),
	order_date date,
	order_id varchar(25),
	ship_date date,
	ship_mode varchar(25),
	shipping varchar(30),
	postal_code int,
	credit_card_type varchar(40),
	credit_card_number bigint,
	customer_feedback varchar(20),
	quarter_number int,
    primary key (shipper_id, order_id, product_id, customer_id)
);

create table shipper_t(
	shipper_id int, 
    shipper_name varchar(50), 
    shipper_contact_details varchar (30),
    primary key (shipper_id)
);

create table product_t(
	product_id int,
    vehicle_maker varchar(60), 
    vehicle_model varchar(60),
    vehicle_color varchar(60),
    vehicle_model_year int, 
    vehicle_price decimal(14,2),
    primary key (product_id)
);

create table order_t (
	order_id varchar(25),
    customer_id varchar(25), 
    shipper_id int, 
    product_id int, 
    quantity int, 
    vehicle_price decimal(10,2),
    order_date date,
    ship_date date,
    discount decimal (4,2),
    ship_mode varchar(25),
    shipping varchar(30),
    customer_feedback varchar(20),
    quarter_number int,
    primary key (order_id)
);

create table customer_t(
	customer_id varchar(25),
	customer_name varchar(25),
	gender varchar(15),
	job_title varchar(50),
	phone_number varchar(20),
	email_address varchar(50),
	city varchar(25),
    country varchar(40),
	State varchar(50),
	customer_address varchar(50),
	postal_code int,
    credit_card_type varchar(40),
    credit_card_number bigint,
    primary key (customer_id)
);


/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Stored Procedures Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [4] Creating the Stored Procedures:

   delimiter $$
create procedure vehicles_p()
begin
	insert into Vehdb.vehicles_t (
    	shipper_id,
		shipper_name,
		shipper_contact_details,
		product_id,
		vehicle_maker,
		vehicle_model,
		vehicle_color,
		vehicle_model_year,
		vehicle_price,
		quantity,
		discount,
		customer_id,
		customer_name,
		gender,
		job_title,
		phone_number,
		email_address,
		city,
		country,
		state,
		customer_address,
		order_date,
		order_id,
		ship_date,
		ship_mode,
		shipping,
		postal_code,
		credit_card_type,
		credit_card_number,
		customer_feedback,
		quarter_number
	) SELECT * FROM Vehdb.temp_t;
end; 

delimiter $$
create procedure shipper_p()
begin 
	insert into Vehdb.shipper_t (
		shipper_id, 
        shipper_name, 
        shipper_contact_details
	)
    select distinct 
    shipper_id, 
		shipper_name, 
        shipper_contact_details
	from Vehdb.vehicles_t where shipper_id not in (select distinct shipper_id from Vehdb.shipper_t);
end;

delimiter $$
create procedure product_p()
begin
	insert into Vehdb.product_t (
		product_id, 
        vehicle_maker, 
        vehicle_model, 
        vehicle_color, 
        vehicle_model_year, 
        vehicle_price
	)
    select distinct 
	product_id, 
        vehicle_maker, 
        vehicle_model, 
        vehicle_color, 
        vehicle_model_year, 
        vehicle_price
	from Vehdb.vehicles_t where product_id not in (select distinct product_id from Vehdb.product_t);
end; 

delimiter $$
create procedure order_p(quarternum int)
begin
	insert into Vehdb.order_t (
		order_id, 
        customer_id, 
        shipper_id, 
        product_id, 
        quantity, 
        vehicle_price, 
        order_date, 
        ship_date, 
        discount, 
        ship_mode, 
        shipping, 
        customer_feedback, 
        quarter_number 
	)
    select distinct 
	order_id, 
        customer_id, 
        shipper_id, 
        product_id, 
        quantity, 
        vehicle_price, 
        order_date, 
        ship_date, 
        discount, 
        ship_mode, 
        shipping, 
        customer_feedback, 
        quarter_number
	from Vehdb.vehicles_t where quarter_number = quarternum;
end;

delimiter $$
create procedure customer_p()
begin
	insert into Vehdb.customer_t (
		customer_id,
		customer_name,
		gender,
		job_title,
		phone_number,
		email_address,
		city,
		country,
		State,
		customer_address,
		postal_code,
		credit_card_type,
		credit_card_number
	)
    select distinct 
	customer_id,
		customer_name,
		gender,
		job_title,
		phone_number,
		email_address,
		city,
		country,
		State,
		customer_address,
		postal_code,
		credit_card_type,
		credit_card_number
	from Vehdb.vehicles_t where customer_id not in (select distinct customer_id from Vehdb.customer_t);
end;

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Data Ingestion
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [5] Ingesting the data:
-- Note: Revisit the video: Week-2: Data Modeling and Architecture: Ingesting data into the main table

TRUNCATE temp_t;

truncate temp_t;
load data local infile '/Users/alrousan95/Downloads/new_wheels_proj (9)/Data/new_wheels_sales_qtr_4.csv'
INTO TABLE temp_t
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 lines;

call vehicles_p();
call customer_p();
call product_p();
call shipper_p();
call order_p(4);

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Views Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [6] Creating the views:
 veh_ord_cust_vcreate view veh_ord_cust_v as
	select 
		cust.customer_id,
        cust.customer_name, 
        cust.city,
        cust.state,
        cust.credit_card_type, 
        ord.order_id, 
        ord.shipper_id, 
        ord.product_id, 
        ord.quantity,
        ord.vehicle_price, 
        ord.order_date, 
        ord.ship_date, 
        ord.discount,
        ord.customer_feedback,
        ord.quarter_number
	from customer_t as cust
		inner join order_t as ord
			on cust.customer_id = ord.customer_id;
        
create view veh_prod_cust_v as
	select 
		cust.customer_id, 
        cust.customer_name, 
        cust.credit_card_type, 
        cust.state, 
        ord.order_id, 
        ord.customer_feedback, 
        prod.product_id, 
        prod.vehicle_maker, 
        prod.vehicle_model, 
        prod.vehicle_color, 
        prod.vehicle_model_year
	from customer_t as cust
		inner join order_t as ord
    on ord.customer_id = cust.customer_id
		inner join product_t prod
	on prod.product_id = ord.product_id;



/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Functions Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [7] Creating the functions:

delimiter $$
create function calc_revenue_f (vehicle_price int, discount decimal (4,2))
returns decimal 
deterministic 
begin
declare revenue decimal;
set revenue = vehicle_price * discount;
return(revenue);
end;

drop function calc_revenue_f

delimiter $$
create function days_to_ship_f (ship_date date, order_date date)
returns integer
deterministic 
begin
declare date_length int; 
set date_length = datediff(ship_date, order_date);
return (date_length);
end;

drop function days_to_ship_f

/*-----------------------------------------------------------------------------------------------------------------------------------
Note: 
After creating tables, stored procedures, views and functions, attempt the below questions.
Once you have got the answer to the below questions, download the csv file for each question and use it in Python for visualisations.
------------------------------------------------------------------------------------------------------------------------------------ 

  
  
-----------------------------------------------------------------------------------------------------------------------------------

                                                         Queries
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/
  
/*-- QUESTIONS RELATED TO CUSTOMERS
     [Q1] What is the distribution of customers across states?*/

select distinct state, count(customer_id) as "Distribution of cars"
from Vehdb.veh_prod_cust_v
group by 1
order by 2 desc 
;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q2] What is the average rating in each quarter?*/

with abc as 
(
	select quarter_number, 
    case 
    when customer_feedback = 'very bad' then '1'
    when customer_feedback = 'bad' then '2' 
    when customer_feedback = 'okay' then '3'
    when customer_feedback = 'good' then'4' 
    when customer_feedback = 'Very good' then '5' 
    end as ranking 
from Vehdb.veh_ord_cust_v
)
select quarter_number as 'Quarter Number', round(avg(ranking),2) as 'Feedback rate'
from abc
group by 1
order by 1;


/* [Q3] Are customers getting more dissatisfied over time?*/
  with abc2 as 
(
	select quarter_number, customer_feedback,
    case 
    when customer_feedback = 'very bad' then '1'
    when customer_feedback = 'bad' then '2' 
    when customer_feedback = 'okay' then '3'
    when customer_feedback = 'good' then'4' 
    when customer_feedback = 'Very good' then '5' 
    end as per_ranking 
from Vehdb.veh_ord_cust_v
)
select quarter_number 'Quarter Number', customer_feedback, round(count(per_ranking)/10,2) as 'Customer Feedback Percentage'
from abc2
group by 1, 2
order by 1;    

-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q4] Which are the top 5 vehicle makers preferred by the customer?.*/
select vehicle_maker 'Vehicle Maker', count(order_id) as 'Orders'
from Vehdb.veh_prod_cust_v
group by vehicle_maker
order by 2 desc
limit 5;

-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q5] What is the most preferred vehicle make in each state?.*/

select 
count(order_id), 
state, 
vehicle_maker,
rank()over (partition by state order by vehicle_maker) as "Vehicles Ranked by Customers"
from Vehdb.veh_prod_cust_v
group by state, vehicle_maker;

-- ---------------------------------------------------------------------------------------------------------------------------------

/*QUESTIONS RELATED TO REVENUE and ORDERS 

-- [Q6] What is the trend of number of orders by quarters?*/

select quarter_number as 'Quarter Number', count(order_id) as 'Total Number of Orders'
from Vehdb.veh_ord_cust_v
group by 1
order by 2 desc;


-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q7] What is the quarter over quarter % change in revenue?*/
with quarper as 
(
	select
		quarter_number,
		SUM(calc_revenue_f(vehicle_price, discount)) as revenue
	from Vehdb.veh_ord_cust_v
	group by 1
)
SELECT
	quarter_number 'Quarter Number',
    revenue,
    ((revenue - lag(revenue) over (order by quarter_number))/lag(revenue) over(order by quarter_number) * 100) as "Previous Quarter Percentage change"
from quarper;
      
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q8] What is the trend of revenue and orders by quarters?*/

select quarter_number as 'Quarter Number', sum(calc_revenue_f(vehicle_price, discount)) as "Revenue Total", count(order_id) as "Number of Orders"
from Vehdb.veh_ord_cust_v
group by quarter_number order by 1;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* QUESTIONS RELATED TO SHIPPING 
    [Q9] What is the average discount offered for different types of credit cards?*/

select credit_card_type'Credit Card Type', round(avg(discount),2) as "Average Discount"
from Vehdb.veh_ord_cust_v
group by credit_card_type order by 1;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q10] What is the average time taken to ship the placed orders for each quarters?.*/

select quarter_number as 'Quarter Number', round(avg(days_to_ship_f(ship_date, order_date)),2) as "Average Days to Ship"
from Vehdb.veh_ord_cust_v
group by quarter_number order by 1;

-- --------------------------------------------------------Done----------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------------------



