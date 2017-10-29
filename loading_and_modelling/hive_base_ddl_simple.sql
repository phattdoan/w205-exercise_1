DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals
(
	provider_id string,
	hospital_name string,
	address string,
	city string,
	state string,
	zip_code string,
	county_name string,
	phone_number string,
	hospital_type string,
	hospital_ownership string,
	emergency_services string,
	meets_criteria string,
	hospital_overall_rating string,
	hospital_overall_rating_footnote string,
	mortality_national_comparison string,
	mortality_national_comparison_footnote string,
	safety_of_care_national_comparison string,
	safety_of_care_national_comparison_footnote string,
	readmission_national_comparison string,
	readmission_national_comparison_footnote string,
	patient_experience_national_comparison string,
	patient_experience_national_comparison_footnote string,
	effectiveness_of_care_national_comparison string,
	effectiveness_of_care_national_comparison_footnote string,
	timeliness_of_care_national_comparison string,
	timeliness_of_care_national_comparison_footnote string,
	efficient_use_of_medical_imaging_national_comparison string,
	efficient_use_of_medical_imaging_national_comparison_footnote string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
	"separatorChar" = ',',
	"quoteChar" = '"',
	"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';