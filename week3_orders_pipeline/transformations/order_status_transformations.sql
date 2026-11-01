CREATE OR REFRESH STREAMING TABLE orders_status_bronze_st
COMMENT 'Raw order status events ingested from JSON files'
TBLPROPERTIES (
    'pipelines.reset.allowed' = 'false'
)         
AS
SELECT *,_metadata.file_path as source_file,
current_timestamp() as ingestion_time
FROM STREAM (read_files(
    "/Volumes/databricks_labs/default/lab_data/week3/status/",
    format => "json"
)
);


CREATE OR REFRESH STREAMING TABLE orders_status_silver_st(
    CONSTRAINT valid_order_id EXPECT(order_id is not null) ON VIOLATION DROP ROW
)
AS
SELECT * EXCEPT (status_timestamp),CAST(status_timestamp AS TIMESTAMP) as status_timestamp
FROM STREAM (orders_status_bronze_st);


CREATE OR REFRESH MATERIALIZED VIEW orders_with_status_mv
AS
SELECT A.order_id,A.customer_id,A.order_timestamp,B.order_status,B.status_timestamp FROM orders_silver_st as A
LEFT JOIN orders_status_silver_st as B
ON A.order_id = B.order_id;