CREATE EXTERNAL TABLE `stedi_project_db`.`customer_landing`(
  `customername` string, 
  `email` string, 
  `phone` string, 
  `birthday` string, 
  `serialnumber` string, 
  `registrationdate` bigint, 
  `lastupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithpublicasofdate` bigint)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
    'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,\
    serialNumber,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lakehouse-mpl/landing_zone/customer'
TBLPROPERTIES ('classification'='json')