-- # TASK 1
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;

-- # TASK 2
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE;

-- # TASK 3

UPDATE dim_store_details
SET
	latitude = NULL
WHERE
	lat = 'N/A';

UPDATE dim_store_details
SET
	longitude = NULL
WHERE
	longitude = 'N/A';

ALTER TABLE dim_store_details
	DROP COLUMN lat,
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,	
	ALTER COLUMN locality TYPE VARCHAR(25),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(25),
	ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(25);

-- #Task 4

UPDATE dim_products
	SET product_price = RIGHT(product_price, LENGTH(product_price) - 1);

ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products	
	SET weight_class = Light
	WHERE weight < 2;
UPDATE dim_products
	SET weight_class = Mid_Sized
	WHERE 2 <= weight AND weight < 40;
UPDATE dim_products
	SET weight_class = Heavy
	WHERE 40 <= weight AND weight < 140;
UPDATE dim_products
	SET weight_class = Truck_Required
	WHERE weight >= 140;
SELECT weight_class, weight FROM dim_products;

UPDATE dim_products	
	SET weight_class = 'Light'
	WHERE weight < 2;
UPDATE dim_products
	SET weight_class = 'Mid_Sized'
	WHERE 2 <= weight AND weight < 40;
UPDATE dim_products
	SET weight_class = 'Heavy'
	WHERE 40 <= weight AND weight < 140;
UPDATE dim_products
	SET weight_class = 'Truck_Required'
	WHERE weight >= 140;

ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN removed TYPE BOOL USING removed::boolean;

UPDATE dim_products
	SET removed = True
	WHERE removed = 'Removed';
UPDATE dim_products
	SET removed = False
	WHERE removed = 'Still_available';

-- #TASK 5

ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

TASK 6

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(22),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE;




COLUMN NAMES - date_uuid, user_uuid, product_quantity, index, card_number, store_code, product_code

dim_card_details - card_number
dim_date_times - date_uuid
dim_products - product_code
dim_store_details - store_code
dim_users - user_uuid

ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);

=======

ALTER TABLE orders_table
	ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);


	=======================

	QUERIES

TASK 1

	SELECT country_code AS country, COUNT(country_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_no_stores DESC;
-- NB one extra GB than necessary

TASK 2

SELECT locality, COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
HAVING COUNT(locality) >= 10
ORDER BY total_no_stores DESC;


TASK 3

SELECT SUM(dim_products.product_price * product_quantity) AS total_sales, dim_date_times.month
FROM orders_table
INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY month
ORDER BY total_sales DESC;


TASK 4:

SELECT 	COUNT(product_quantity) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
		CASE
			WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END AS location	
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY location DESC;

TASK 5:

SELECT 	dim_store_details.store_type,
		SUM(product_quantity * dim_products.product_price) AS total_sales,
		SUM(product_quantity * dim_products.product_price) * 100 /
		(SELECT SUM(product_quantity * dim_products.product_price)
		 FROM orders_table
		 JOIN dim_products ON dim_products.product_code = orders_table.product_code)
 		AS percentage_total
FROM orders_table
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY store_type
ORDER BY total_sales DESC;

TASK 6:

SELECT
	ROUND(SUM(product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
	dim_date_times.year,
	dim_date_times.month
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC;

TASK 7:

SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

TASK 8:

SELECT ROUND(SUM(dim_products.product_price * product_quantity)::numeric, 2) AS total_sales, dim_store_details.store_type, dim_store_details.country_code
FROM orders_table
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.store_type, dim_store_details.country_code
HAVING dim_store_details.country_code = 'DE'
ORDER BY total_sales;

TASK 9:

WITH cte3 AS(
	WITH cte2 AS (
		WITH cte AS(
			SELECT	year,
					month,
					day,
					CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS new_time
			FROM dim_date_times)
		SELECT	year,
				month,
				day,
				new_time,
				LEAD(new_time, 1) OVER (ORDER BY new_time ASC) AS next_time
		FROM cte)
	SELECT	year,
			AVG(next_time - new_time) AS actual_time_taken
	FROM cte2
	GROUP BY year
	ORDER BY actual_time_taken DESC)
SELECT	year,
		CONCAT('"hours: "', DATE_PART('hour', actual_time_taken), ', "minutes": ', DATE_PART('minute', actual_time_taken), ', "seconds": ', DATE_PART('second', actual_time_taken), ', "milliseconds: "', DATE_PART('millisecond', actual_time_taken)) AS actual_time_taken
FROM cte3;