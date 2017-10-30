### Phat Doan
### w205 Section 2 - Fall 2017
### Exercise 1

**Question to answer**
1/ What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
2/ What states are models of high-quality care?
3/ Which procedures have the greatest variability between hospitals?
4/ Are average scores for hospital quality or procedural variability correlated with patient survey responses?


## Project plan & notes:
. Identify key metrics for hospital and state evaluation as the models of high-quality care:
	- Understand the data source
		- Hospital (hospitals.csv)
			- Contains all type of hospitals (i.e. acute care, emmergencies, level-1 trauma, etc)
			- For comparison, would need to segment and focus on a specific type of hospitals:
				- Size
				- Population serving
				- Population proportion
				- Type of services provided
		- High-quality care metrics:
			- Readmission rate
			- Patient satisfaction
			- Cost of care
			- HCAHPS Hospital consumer assessment of healthcare providers and systems
			- Total Performance Score
			- MSPB Medicare Spending Per Beneficiary
		- Procedure variety
			- AMI Acute Myocardial Infarction
			- CABG Coronary Artery Bypass Graft
			- CAUTI Catheter-associated urinary tract infections
			- CDI Clostridium difficile Infection
			- CJR Comprehensive Care Joint Replacement
			- EDAC Excess days in acute care
			- HAI Healthcare-associated infections
			- HBIPS Hospital-Based inpatient psychiatreic services
			- HF Heart Failure
			- HIP-KNEE TKA
			- IMG Imaging
			- complMORT Mortality
			- MRSA
			- OCM Oncology Care Measures
			- OP Outpatient
			- OQR Oupatient Quality Reporting
			- PN Pneumonia
			- STK Stroke
	- Measures have different dates. Most measures have completed data set for 2015, hence all evaluation will focus on the year of 2015
. Selected tables to be loaded into a data lake
	- Hospital General Information.csv -> hospitals.csv
		- Table is at hospital level detail that contains all hospital details
		- 4,813 hospitals
		- To be utilized as the main table for all references
		- Key: Provider_ID
		- Varibales to consider:
			- Hospital type
			- Hospital owner
			- Ratings
		- Transform table by trimming unnecessary variables
	- Complications and Deaths – Hospital.csv -> complications.csv
		- Table is at hospital/measure name level
		- Key: Provider_ID
		- Variable dictionary:
			tha_tka_complications: Rate of complications for hip/knee replacement patients
			ami_mortality: Acute Myocardial Infarction (AMI) 30-Day Mortality Rate
			cabg_mortality: Death rate for CABG
			copd_mortality: Death rate for chronic obstructive pulmonary disease (COPD) patients
			hf_mortality: Heart failure (HF) 30-Day Mortality Rate
			pneumonia_mortality: Pneumonia (PN) 30-Day Mortality Rate
			stroke_mortality: Death rate for stroke patients
			blood_clots: Serious blood clots after surgery
			blood_infections: Blood stream infection after surgery
			wound_open: A wound that splits open  after surgery on the abdomen or pelvis
			cut_tears: Accidental cuts and tears from medical treatment
			pressure_sores: Pressure sores
			complications_mortality: Deaths among Patients with Serious Treatable Complications after Surgery
			collapsed_lung: Collapsed lung due to medical treatment
			catheter_infections: Infections from a large venous catheter
			broken_hip_fell: Broken hip from a fall after surgery
			serious_complications: Serious complications
		- Tranformation: Pivot to have variables with "compared to national" as observation
	- HCAHPS – Hospital.csv -> hcahps.csv
	- Healthcare Associated Infections - Hospital.csv -> hai.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variable dictionary
			clabsi_device_days: CLABSI: Number of Device Days
			clabsi_predicted: CLABSI: Predicted Cases
			clabsi_observed: CLABSI: Observed Cases
			cauti_day: CAUTI: Number of Urinary Catheter Days
			cauti_predicted: CAUTI: Predicted Cases
			cauti_observed: CAUTI: Observed Cases
			ssi_colon_procedures: SSI: Colon, Number of Procedures
			ssi_predicted: SSI: Colon Predicted Cases
			ssi_observed: SSI: Colon Obsesved Cases
			ssi_ab_procedures: SSI: Abdominal, Number of Procedures
			ssi_ab_predicted: SSI: Abdominal Predicted Cases
			ssi_ab_observed: SSI: Abdominal Observed Cases
			mrsa_patient_days: MRSA Patient Days
			mrsa_predicted: MRSA Predicted Cases
			mrsa_observed: MRSA Observed Cases
			cdiff_patient_days: C.diff Patient Days
			cdiff_predicted: C.diff Predicted Cases
			cdiff_observed: C.diff Observed Cases
	- Hospital Returns – Hospital.csv -> hosreturns.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variable dictionary:
			heart_attack_return_days: Hospital return days for heart attack patients
			heart_failure_return_days: Hospital return days for heart failure patients
			ami_readmission_rate: Acute Myocardial Infarction (AMI) 30-Day Readmission Rate
			cabg_readmission_rate:Rate of readmission for CABG
			copd_readmission_rate: Rate of readmission for chronic obstructive pulmonary disease (COPD) patients
			hf_readmission_rate: Heart failure (HF) 30-Day Readmission Rate
			tka_readmission_rate: Rate of readmission after hip/knee replacement
			discharge_readmission_rate: Rate of readmission after discharge from hospital (hospital-wide)
			pneumonia_readmission_rate: Pneumonia (PN) 30-Day Readmission Rate
			stroke_readmission_rate: Rate of readmission for stroke patients
	- Timely and Effective Care - Hospital.csv -> care.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variable dictionary:
			er_volume: Emergency department volume
			pop_flu_immunization: Immunization for influenza
			worker_flu_immunization: Healthcare workers given influenza vaccination
			fibrinolysis_median_time: Median Time to Fibrinolysis
			fibrinolysis_within_30_min: Fibrinolytic Therapy Received Within 30 Minutes of ED Arrival
			door_to_diagnostic: Door to diagnostic eval
			pain_med_median_time: Median time to pain med
			left_before_being_seen: Left before being seen
			cataract_visual_improvement: Improvement in Patient’s Visual Function within 90 Days Following Cataract Surgery
			facility_transfer_median_time: Median Time to Transfer to Another Facility for Acute coronary_intervention: Coronary Intervention
			ecg_median_time: Median Time to ECG
			newborns_schedule_early_percentage: Percent of newborns whose deliveries were scheduled early (1-3 weeks early), when a scheduled delivery was not medically necessary
			warfarin_discharge_instructions: Warfarin therapy discharge instructions
			hai_preventabl_venous_thromboembolism: Hospital acquired potentially preventable venous thromboembolism
	- hvbp_hcahps_11_10_2016 -> surveys.csv
		- Table is at hospital level in a wide-format
		- Key: Provider Number
		- Variable dictionary:
			comm_nurses_floor: Communication with Nurses Floor
			comm_nurses_benchmark: Communication with Nurses Benchmark
			comm_nurses_baseline: Communication with Nurses Baseline Rate
			comm_nurses_performance: Communication with Nurses Performance Rate
			comm_nurses_achievement: Communication with Nurses Achievement Points
			comm_nurses_improvement: Communication with Nurses Improvement Points
			comm_nurses_dimension: Communication with Nurses Dimension Score
			comm_doctors_floor: Communication with Doctors Floor
			comm_doctors_benchmark: Communication with Doctors Benchmark
			comm_doctors_baseline: Communication with Doctors Baseline Rate
			comm_doctors_performance: Communication with Doctors Performance Rate
			comm_doctors_achievement: Communication with Doctors Achievement Points
			comm_doctors_improvement: Communication with Doctors Improvement Points
			comm_doctors_dimension: Communication with Doctors Dimension Score
			comm_medicines_floor: Communication about Medicines Floor
			comm_medicines_benchmark: Communication about Medicines Benchmark
			comm_medicines_baseline: Communication about Medicines Baseline Rate
			comm_medicines_performnaces: Communication about Medicines Performance Rate
			comm_medicines_achievement: Communication about Medicines Achievement Points
			comm_medicines_improvement: Communication about Medicincs Improvement Points
			comm_medicines_dimension: Communication about Medicines Dimension Score
			responsiveness_staff_floor: Responsiveness of Hospital Staff Floor
			responsiveness_staff_benchmark: Responsiveness of Hospital Staff Benchmark
			responsiveness_staff_baseline: Responsiveness of Hospital Staff Baseline Rate
			responsiveness_staff_performance: Responsiveness of Hospital Staff Performance Rate
			responsiveness_staff_achievement: Responsiveness of Hospital Staff Achievement Points
			responsiveness_staff_improvement: Responsiveness of Hospital Staff Improvement Points
			responsiveness_staff_dimension: Responsiveness of Hospital Staff Dimension Score
			pain_management_floor: Pain Management Floor
			pain_management_benchmark: Pain Management Benchmark
			pain_management_baseline: Pain Management Baseline Rate
			pain_management_performance: Pain Management Performance Rate
			pain_management_achievement: Pain Management Achievement Points
			pain_management_improvement: Pain Management Improvement Points
			pain_management_dimension: Pain Management Dimension Score
			clean_quiet_floor: Cleanliness and Quietness of Hospital Environment Floor
			clean_quiet_benchmark:Cleanliness and Quietness of Hospital Environment Benchmark
			clean_quiet_baseline: Cleanliness and Quietness of Hospital Environment Baseline Rate
			clean_quiet_performance: Cleanliness and Quietness of Hospital Environment Performance Rate
			clean_quiet_achievement: Cleanliness and Quietness of Hospital Environment Achievement Points
			clean_quiet_improvement: Cleanliness and Quietness of Hospital Environment Improvement Points
			clean_quiet_dimension: Cleanliness and Quietness of Hospital Environment Dimension Score
			discharge_floor: Discharge Information Floor
			discharge_benchmark: Discharge Information Benchmark
			discharge_baseline: Discharge Information Baseline Rate
			discharge_performance: Discharge Information Performance Rate
			discharge_achievement: Discharge Information Achievement Points
			discharge_improvement: Discharge Information Improvement Points
			discharge_dimension: Discharge Information Dimension Score
			overall_rating_floor: Overall Rating of Hospital Floor
			overall_rating_benchmark: Overall Rating of Hospital Benchmark
			overall_rating_baseline: Overall Rating of Hospital Baseline Rate
			overall_rating_performance: Overall Rating of Hospital Performance Rate
			overall_rating_achievement: Overall Rating of Hospital Achievement Points
			overall_rating_improvement: Overall Rating of Hospital Improvement Points
			overall_rating_dimension: Overall Rating of Hospital Dimension Score
			hcahps_base_score: HCAHPS Base Score
			hcahps_consistency_score: HCAHPS Consistency Score
	- Measure Dates.csv -> measures.csv
		- Contain key mapping for measures
		- Key: Measure ID
. Build schema based on available data
. Transform the data
. EDA questions
	- What is the distribution of "Hospital overall rating" in the hospital.csv?

. Problems with the data:
	- From CMS, data might be skewed that represent a Medicare population rather than a truly generalized American population



## Week 1: Setting up the data lake
Files:
. Load data_lake: https://raw.githubusercontent.com/phattdoan/UCB-205-HDFS-HIVE-SPARK/master/load_data_lake.sh

Setting up environment:
. Start AWS
. Attached volume & start HDFS + Postgres
	Find the volume location, by typing `fdisk -l`
   
	   . Mount the volume as follows: `mount -t ext4 /dev/xvdf /data`
	   . Start HDFS, Hadoop Yarn and Hive: `/root/start-hadoop.sh`
	   . Start Postgres: `/data/start_postgres.sh`
	   . Start Hive metastore
	   	- /data/start_metastore.sh
	   	- /data/spark15/bin/spark-sql

. switch to `su w205`

. Run git_load_remove.sh to clone the git repo for execution of loading_and_modelling
	
. Stop hadoop and postgres
	. Stop hive 
		- ps -ef|grep metastore
		- kill <id>
	.`/root/stop-hadoop.sh`
	.`/data/stop_postgres.sh`


## Week 2: transforming data lake
.hospital_info: contain information and CMS-aggregated measures
	provider_id,
	hospital_name,
	city,
	state,
	zip_code,
	hospital_type,
	hospital_ownership,
	emergency_services,
	meets_criteria,
	hospital_overall_rating,
	mortality_national_comparison,
	safety_of_care_national_comparison,
	readmission_national_comparison,
	patient_experience_national_comparison,
	effectiveness_of_care_national_comparison,
	timeliness_of_care_national_comparison,
	efficient_use_of_medical_imaging_national_comparison

.hospital_baseline:

## Week 3: investigating
. Hospitals: 
	. There are 3 types of hospitals observed from the data:
		Childrens: 99
		Acute Care hospitals: 3369
		Critical Access hospitals: 1344 - critical access hospital is a designation given to certain rural hospitals by the Centers for Medicare and Medicaid Services (CMS)
	. With emergency services
		No      238
		Yes     3131
	. With meets criteria:
		Not Available   1
		Y       3075
		        55
	For this review purpose, I would focus on Acute Care hospitals with emergency services and meet criteria since childrens and critical access hospitals serve different purposes as well as different measuring metrics
.States: including on acute care hospitals, not surprisingly most hospitals concentrated in states with dense population
	AK      8
	AL      84
	AR      45
	AS      1
	AZ      63
	CA      298
	CO      48
	CT      30
	DC      7
	DE      6
	FL      170
	GA      101
	GU      2
	HI      12
	IA      34
	ID      14
	IL      126
	IN      85
	KS      52
	KY      65
	LA      92
	MA      57
	MD      47
	ME      17
	MI      94
	MN      50
	MO      72
	MP      1
	MS      63
	MT      14
	NC      86
	ND      8
	NE      23
	NH      13
	NJ      64
	NM      32
	NV      22
	NY      152
	OH      128
	OK      86
	OR      34
	PA      150
	PR      51
	RI      11
	SC      54
	SD      21
	TN      90
	TX      315
	UT      32
	VA      75
	VI      2
	VT      6
	WA      49
	WI      66
	WV      29
	WY      12

[OBSOLETE]
. Get the latest dataset: https://data.medicare.gov/views/bg9k-emty/files/4a66c672-a92a-4ced-82a2-033c28581a90?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip

. Tables:
	. General Hospital Information
	. "Timeline and Effective Care - Hospital.csv" - procedure data
	. "Readmissions and Deaths - Hospital.csv" - procedure data
	. "Measure Dates.csv" - mapping of measures to codes
	. "hvbp_hca_hps_05_28_2015.csv" - survey response data
	. "COMPLICATIONS-HOSPITAL.csv" - complications
	. "HEALTHCARE ASSOCIATED INFECTIONS-HOSPITALS.csv"
	. "PAYMENT-HOSPITAL.csv" - payment and value of care
	. "MEDICARE HOSPITAL SPENDING PER PATIENT-HOSPITAL.csv" - medicare spending per beneficiary

. Clean files and remove first lines4

. Load files into HDFS
	. hdfs dfs -put userdata_lab.csv /user/w205/lab_3/user_data
	. hdfs dfs -put weblog_lab.csv /user/w205/lab_3/weblog_data



Git setup:

load_data_lake.sh
chmod u+x,g+x load_data_lake.sh


Staging:
```
mkdir staging
cd staging
mkdir exercise_1
echo $MY_URL = "https://data.medicare.gov/views/bg9k-emty/files/4a66c672-a92a-4ced-82a2-033c28581a90?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip"
set -o vi
wget "$MY_URL" -O medicare_data.zip
unzip medicare_data.zip
ls -l Hosp*.csv
MY_FILE = "Hospital General Informcation.csv"
head -5 "$MY_FILE"
tail -n +2 "$MY_FILE" > hospitals.css # to remove header
```

Hadoop loading:
hdfs dfs -ls /user/w205
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put hospitals.csv /user/w205/hospital_compare


QA:
check exercise1 directory
chec


```
For Exercise 1, if you are using the latest dataset, there are a couple of names that have changed.  
data.medicare.gov
Hospital Compare Dataset
Updated: 7/26/2017
Name changes:
Hospital General Information.csv => same
Timely and Effective Case - Hospital.csv => same
Readmissions and Deaths - Hospital.csv => Complications and Deaths - Hospital.csv
Measure Dates.csv => same
hvbp_hcahps_05_28_2015.csv => hvbp_hcahps_11_10_2016.csv
```



comm_nurses_floor
comm_nurses_benchmark
comm_nurses_baseline
comm_nurses_performance
comm_nurses_achievement
comm_nurses_improvement
comm_nurses_dimension
comm_doctors_floor
comm_doctors_benchmark
comm_doctors_baseline
comm_doctors_performance
comm_doctors_achievement
comm_doctors_improvement
comm_doctors_dimension
comm_medicines_floor
comm_medicines_benchmark
comm_medicines_baseline
comm_medicines_performnaces
comm_medicines_achievement
comm_medicines_improvement
comm_medicines_dimension
responsiveness_staff_floor
responsiveness_staff_benchmark
responsiveness_staff_baseline
responsiveness_staff_performance
responsiveness_staff_achievement
responsiveness_staff_improvement
responsiveness_staff_dimension
pain_management_floor
pain_management_benchmark
pain_management_baseline
pain_management_performance
pain_management_achievement
pain_management_improvement
pain_management_dimension
clean_quiet_floor
clean_quiet_benchmark
clean_quiet_baseline
clean_quiet_performance
clean_quiet_achievement
clean_quiet_improvement
clean_quiet_dimension
discharge_floor
discharge_benchmark
discharge_baseline
discharge_performance
discharge_achievement
discharge_improvement
discharge_dimension
overall_rating_floor
overall_rating_benchmark
overall_rating_baseline
overall_rating_performance
overall_rating_achievement
overall_rating_improvement
overall_rating_dimension
hcahps_base_score
hcahps_consistency_score