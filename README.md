NationPulse Data Ingestion Service With Go

CREATE OR REPLACE PROCEDURE create_population_table(
table_name VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
-- Create table if it doesn't exist
EXECUTE format('
CREATE TABLE IF NOT EXISTS %I (
id SERIAL PRIMARY KEY,
country_code VARCHAR(10) NOT NULL,
country_name VARCHAR(100) NOT NULL,
indicator_code VARCHAR(50) NOT NULL,
indicator VARCHAR(50) NOT NULL,
sex_code VARCHAR(5) NOT NULL,
sex_name VARCHAR(10) NOT NULL,
age VARCHAR(100) NOT NULL,
year INTEGER NOT NULL,
value DECIMAL NOT NULL,
last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
 UNIQUE(country_code, indicator, year)  
 )', table_name);

    RAISE NOTICE 'Table % created successfully', table_name;

END;

$$
;

CREATE OR REPLACE PROCEDURE create_economy_gov_table(
    table_name VARCHAR
)
LANGUAGE plpgsql
AS
$$

BEGIN
-- Create table if it doesn't exist
EXECUTE format('
CREATE TABLE IF NOT EXISTS %I (
id SERIAL PRIMARY KEY,
country_code VARCHAR(10) NOT NULL,
country_name VARCHAR(100) NOT NULL,
indicator_code VARCHAR(50) NOT NULL,
indicator VARCHAR(50) NOT NULL,
year INTEGER NOT NULL,
value DECIMAL NOT NULL,
last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
 UNIQUE(country_code, indicator, year)  
 )', table_name);

    RAISE NOTICE 'Table % created successfully', table_name;

END;

$$
;

CREATE OR REPLACE PROCEDURE insert_economy_gdp_data(
    table_name VARCHAR,
    country_code VARCHAR,
    country_name VARCHAR,
    indicator_code VARCHAR,
    indicator VARCHAR,
    year VARCHAR,
    value VARCHAR
)
LANGUAGE plpgsql
AS
$$

BEGIN
-- Insert data
EXECUTE format('
INSERT INTO %I
(country_code, country_name, indicator_code, indicator,year, value)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (country_code, indicator, year)
DO UPDATE SET
value = EXCLUDED.value,
last_updated = CURRENT_TIMESTAMP
', table_name)
USING country_code, country_name, indicator_code, indicator, year, value;

    RAISE NOTICE 'Data inserted into table % successfully', table_name;

END;

$$
;
$$
