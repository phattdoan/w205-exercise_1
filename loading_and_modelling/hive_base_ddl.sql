DROP TABLE my_table;

CREATE EXTERNAL TABLE my_table
(
	my_column_1 string,
	my_column_2 string

)
ROW FORMAT SERD 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
	"separatorChar" = ",",
	"quoteChar" = "",
	"escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATIOn '/user/w205/hospital_compare/';