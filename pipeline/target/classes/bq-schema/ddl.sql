create table `ods.transactions` (
  uid STRING,
  item STRING,
  price FLOAT64,
  transaction_id STRING,
  transaction_ts TIMESTAMP,
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  data JSON,
  attributes JSON
)