CREATE EXTERNAL TABLE `smeranovich_metrics`(
  `fromtimestamp` string COMMENT '', 
  `maxvalue` double COMMENT '', 
  `metricname` string COMMENT '', 
  `minvalue` double COMMENT '', 
  `totimestamp` string COMMENT '', 
  `unit` string COMMENT '')
PARTITIONED BY ( 
  `componentname` string COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://smeranovich/parquet-data'
TBLPROPERTIES (
  'classification'='parquet', 
  'transient_lastDdlTime'='1779605881')
  
ALTER TABLE smeranovich_metrics
    ADD PARTITION (componentname = 'order-service') LOCATION 's3://smeranovich/parquet-data/order-service/';
	
ALTER TABLE smeranovich_metrics
    ADD PARTITION (componentname = 'user-service') LOCATION 's3://smeranovich/parquet-data/user-service/';	