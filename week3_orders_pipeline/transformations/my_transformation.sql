CREATE OR REFRESH STREAMING TABLE orders_bronze_st AS
SELECT
  *,
  _metadata.file_name  AS source_file,
  current_timestamp()  AS ingestion_time
FROM STREAM (read_files(
  "/Volumes/databricks_labs/default/lab_data/week3/orders/",
  format => "json"
));

CREATE OR REFRESH STREAMING TABLE orders_silver_warn_st(
  CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)    
)
AS
SELECT
  customer_id,
  notifications,
  order_id,
  CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
  source_file,
  ingestion_time,
  CAST(order_timestamp AS DATE) AS order_date
FROM STREAM(orders_bronze_st);

CREATE OR REFRESH STREAMING TABLE orders_silver_st (
  CONSTRAINT valid_order_id    EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  customer_id,
  notifications,
  order_id,
  CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
  source_file,
  ingestion_time,
  CAST(order_timestamp AS DATE) AS order_date
FROM STREAM(orders_bronze_st);

CREATE OR REFRESH MATERIALIZED VIEW orders_gold_st 
AS
select 
    order_date,
    count(order_id) as total_orders,
    count(distinct customer_id) as unique_customers
from orders_silver_st
group by order_date
order by order_date;



