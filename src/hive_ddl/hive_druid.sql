
drop table IF EXISTS hotel_enquiries_csv;
drop table IF EXISTS hotel_enquiries_druid;

create external table hotel_enquiries_csv ( 
check_in string, 
check_out string, 
number_rooms int ,
date_of_enquiry_timestamp bigint, 
metro string, 
property string, 
number_of_audults int, 
number_of_childern int, 
date_of_enquiry string, 
`timestamp` bigint,
quoted_price int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/hotelenquiries/'
tblproperties ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE hotel_enquiries_druid
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler' TBLPROPERTIES ("druid.segment.granularity"="MONTH","druid.query.granularity"="DAY") AS SELECT cast(date_of_enquiry_timestamp as timestamp) as `__time`,date_of_enquiry,check_in,check_out,metro,property, cast(quoted_price as string) as quoted_price_s ,from_unixtime(unix_timestamp(check_in,'mm-dd-yyyy'),'E') as check_in_week, number_rooms,number_of_audults,number_of_childern FROM hotel_enquiries_csv where  date_of_enquiry_timestamp is not null;





drop table IF EXISTS hotel_transactions_csv;
drop table IF EXISTS hotel_transactions_druid;

create external table hotel_transactions_csv ( 
check_in string, 
check_out string, 
number_rooms int ,
transaction_dt_timestamp bigint, 
metro string, 
property string, 
number_of_audults int, 
number_of_childern int, 
transaction_dt string, 
`timestamp` bigint,
price_paid int,
customer_id int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/hoteltransactions/'
tblproperties ("skip.header.line.count"="1");


CREATE external TABLE hotel_transactions_druid
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler' TBLPROPERTIES ("druid.segment.granularity"="MONTH","druid.query.granularity"="DAY") AS SELECT cast(transaction_dt_timestamp as timestamp) as `__time`,transaction_dt,check_in,check_out,metro,property, cast(price_paid as string) as price_paid_s ,from_unixtime(unix_timestamp(transaction_dt,'mm-dd-yyyy'),'E') as transaction_in_week, number_rooms,number_of_audults,number_of_childern FROM hotel_transactions_csv where transaction_dt_timestamp is not null;




CREATE EXTERNAL TABLE kafka_hotel_enquiries_druid(
`__time` timestamp,
check_in string,
check_out string,
metro string,
property string,
quoted_price string,
number_of_audults string,
number_of_childern string,
number_rooms string)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
"kafka.bootstrap.servers" = "ip-172-31-26-98.ec2.internal:6667",
"kafka.topic" = "hotel_enquiries",
"druid.kafka.ingestion.useEarliestOffset" = "true",
"druid.kafka.ingestion.maxRowsInMemory" = "5",
"druid.kafka.ingestion.startDelay" = "PT1S",
"druid.kafka.ingestion.period" = "PT1S",
"druid.kafka.ingestion.consumer.retries" = "2",
"druid.kafka.ingestion" = 'START'
)



CREATE EXTERNAL TABLE kafka_hotel_transactions_druid(
`__time` timestamp,
check_in string,
check_out string,
metro string,
property string,
paid_price string,
customer_id string,
number_of_audults string,
number_of_childern string,
number_rooms string)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
"kafka.bootstrap.servers" = "ip-172-31-26-98.ec2.internal:6667",
"kafka.topic" = "hotel_transactions",
"druid.kafka.ingestion.useEarliestOffset" = "true",
"druid.kafka.ingestion.maxRowsInMemory" = "5",
"druid.kafka.ingestion.startDelay" = "PT1S",
"druid.kafka.ingestion.period" = "PT1S",
"druid.kafka.ingestion.consumer.retries" = "2",
"druid.kafka.ingestion" = 'START'
)


