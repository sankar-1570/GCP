--create and load the data from bigquery public dataset sales. size - 8.5gb

create table if not exists test_ds.sales_data as select * FROM bigquery-public-data.iowa_liquor_sales.sales


--creating new table with partition with date cloumn.  
create table test_ds.sales_data_p(
    invoice_and_item_number STRING,
    date DATE,
    store_number STRING,
    store_name STRING,
    address STRING,
    city STRING,
    zip_code STRING,
    store_location GEOGRAPHY,
    county_number STRING,
    county STRING,
    category STRING,
    category_name STRING,
    vendor_number STRING,
    vendor_name STRING,
    item_number STRING,
    item_description STRING,
    pack INTEGER,
    bottle_volume_ml INTEGER,
    state_bottle_cost FLOAT64,
    state_bottle_retail FLOAT64,
    bottles_sold INTEGER,
    sale_dollars FLOAT64,
    volume_sold_liters FLOAT64,
    volume_sold_gallons FLOAT64
  )
partition by date
-- creating new table with partition with date cloumn and clustering with store_number and city,zip_code
create table test_ds.sales_data_p&c(
    invoice_and_item_number STRING,
    date DATE,
    store_number STRING,
    store_name STRING,
    address STRING,
    city STRING,
    zip_code STRING,
    store_location GEOGRAPHY,
    county_number STRING,
    county STRING,
    category STRING,
    category_name STRING,
    vendor_number STRING,
    vendor_name STRING,
    item_number STRING,
    item_description STRING,
    pack INTEGER,
    bottle_volume_ml INTEGER,
    state_bottle_cost FLOAT64,
    state_bottle_retail FLOAT64,
    bottles_sold INTEGER,
    sale_dollars FLOAT64,
    volume_sold_liters FLOAT64,
    volume_sold_gallons FLOAT64
  )
partition by date
cluster by city,store_number,zip_code
  
  
--load data from non partition table to new partition table   

insert into test_ds.sales_data_p select * from test_ds.sales_data; -- loading data to the partitioning table

insert into test_ds.sales_data_pc select * from test_ds.sales_data; -- loading data to the partitioning with clustering table
  
-- bellow select queries will show the diff b/w partition&clusturing and non parition table

select * from test_ds.sales_data where date='2013-02-20' -- non partitioning table

select * from test_ds.sales_data_p where date='2013-02-20' -- partitioning table

select * from test_ds.sales_data_pc where date='2013-02-20' -- partitioning with clusturing table
