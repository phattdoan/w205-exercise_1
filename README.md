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
			- MORT Mortality
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
		- Variables to consider:
			Rate of complications for hip/knee replacement patients
			Acute Myocardial Infarction (AMI) 30-Day Mortality Rate
			Death rate for CABG
			Death rate for chronic obstructive pulmonary disease (COPD) patients
			Heart failure (HF) 30-Day Mortality Rate
			Pneumonia (PN) 30-Day Mortality Rate
			Death rate for stroke patients
			Serious blood clots after surgery
			Blood stream infection after surgery
			A wound that splits open  after surgery on the abdomen or pelvis
			Accidental cuts and tears from medical treatment
			Pressure sores
			Deaths among Patients with Serious Treatable Complications after Surgery
			Collapsed lung due to medical treatment
			Infections from a large venous catheter
			Broken hip from a fall after surgery
			Serious complications
		- Tranformation: Pivot to have variables with "compared to national" as observation
	- HCAHPS – Hospital.csv -> hcahps.csv
	- Healthcare Associated Infections - Hospital.csv -> hai.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variables to consider
			CLABSI: Lower Confidence Limit
			CLABSI: Upper Confidence Limit
			CLABSI: Number of Device Days
			CLABSI: Predicted Cases
			CLABSI: Observed Cases
			Central line-associated bloodstream infections (CLABSI) in ICUs and select wards
			CAUTI: Lower Confidence Limit
			CAUTI: Upper Confidence Limit
			CAUTI: Number of Urinary Catheter Days
			CAUTI: Predicted Cases
			CAUTI: Observed Cases
			Catheter-associated urinary tract infections (CAUTI) in ICUs and select wards
			SSI: Colon Lower Confidence Limit
			SSI: Colon Upper Confidence Limit
			SSI: Colon, Number of Procedures
			SSI: Colon Predicted Cases
			SSI: Colon Observed Cases
			Surgical Site Infection from colon surgery (SSI: Colon)
			SSI: Abdominal Lower Confidence Limit
			SSI: Abdominal Upper Confidence Limit
			SSI: Abdominal, Number of Procedures
			SSI: Abdominal Predicted Cases
			SSI: Abdominal Observed Cases
			Surgical Site Infection from abdominal hysterectomy (SSI: Hysterectomy)
			MRSA Lower Confidence Limit
			MRSA Upper Confidence Limit
			MRSA Patient Days
			MRSA Predicted Cases
			MRSA Observed Cases
			Methicillin-resistant Staphylococcus Aureus (MRSA) Blood Laboratory-identified Events (Bloodstream infections)
			C.diff Lower Confidence Limit
			C.diff Upper Confidence Limit
			C.diff Patient Days
			C.diff Predicted Cases
			C.diff Observed Cases
			Clostridium difficile (C.diff.) Laboratory-identified Events (Intestinal infections)
	- Hospital Returns – Hospital.csv -> hosreturns.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variables to consider
			Hospital return days for heart attack patients
			Hospital return days for heart failure patients
			Acute Myocardial Infarction (AMI) 30-Day Readmission Rate
			Rate of readmission for CABG
			Rate of readmission for chronic obstructive pulmonary disease (COPD) patients
			Heart failure (HF) 30-Day Readmission Rate
			Rate of readmission after hip/knee replacement
			Rate of readmission after discharge from hospital (hospital-wide)
			Pneumonia (PN) 30-Day Readmission Rate
			Rate of readmission for stroke patients
	- Timely and Effective Care - Hospital.csv -> care.csv
		- Table is at Hospital and Measure level
		- Key: Provider ID
		- Variables to consider
			ED1
			ED2
			Emergency department volume
			Immunization for influenza
			Healthcare workers given influenza vaccination
			Median Time to Fibrinolysis
			OP 18
			Fibrinolytic Therapy Received Within 30 Minutes of ED Arrival
			Door to diagnostic eval
			Median time to pain med
			Left before being seen
			Head CT results
			Endoscopy/polyp surveillance: appropriate follow-up interval for normal colonoscopy in average risk patients
			Endoscopy/polyp surveillance: colonoscopy interval for patients with a history of adenomatous polyps - avoidance of inappropriate use
			Improvement in Patient’s Visual Function within 90 Days Following Cataract Surgery
			Median Time to Transfer to Another Facility for Acute Coronary Intervention
			Aspirin at Arrival
			Median Time to ECG
			Percent of newborns whose deliveries were scheduled early (1-3 weeks early), when a scheduled delivery was not medically necessary
			Thrombolytic Therapy
			Warfarin therapy discharge instructions
			Hospital acquired potentially preventable venous thromboembolism
	- Measure Dates.csv -> measures.csv
		- Contain key mapping for measures
		- Key: Measure ID
	- hvbp_hcahps_11_10_2016 -> surveys.csv
		- Table is at hospital level in a wide-format
		- Key: Provider Number
		- Variables to consider
			Communication with Nurses Floor
			Communication with Nurses Achievement Threshold
			Communication with Nurses Benchmark
			Communication with Nurses Baseline Rate
			Communication with Nurses Performance Rate
			Communication with Nurses Achievement Points
			Communication with Nurses Improvement Points
			Communication with Nurses Dimension Score
			Communication with Doctors Floor
			Communication with Doctors Achievement Threshold
			Communication with Doctors Benchmark
			Communication with Doctors Baseline Rate
			Communication with Doctors Performance Rate
			Communication with Doctors Achievement Points
			Communication with Doctors Improvement Points
			Communication with Doctors Dimension Score
			Responsiveness of Hospital Staff Floor
			Responsiveness of Hospital Staff Achievement Threshold
			Responsiveness of Hospital Staff Benchmark
			Responsiveness of Hospital Staff Baseline Rate
			Responsiveness of Hospital Staff Performance Rate
			Responsiveness of Hospital Staff Achievement Points
			Responsiveness of Hospital Staff Improvement Points
			Responsiveness of Hospital Staff Dimension Score
			Pain Management Floor
			Pain Management Achievement Threshold
			Pain Management Benchmark
			Pain Management Baseline Rate
			Pain Management Performance Rate
			Pain Management Achievement Points
			Pain Management Improvement Points
			Pain Management Dimension Score
			Communication about Medicines Floor
			Communication about Medicines Achievement Threshold
			Communication about Medicines Benchmark
			Communication about Medicines Baseline Rate
			Communication about Medicines Performance Rate
			Communication about Medicines Achievement Points
			Communication about Medicines Improvement Points
			Communication about Medicines Dimension Score
			Cleanliness and Quietness of Hospital Environment Floor
			Cleanliness and Quietness of Hospital Environment Achievement Threshold
			Cleanliness and Quietness of Hospital Environment Benchmark
			Cleanliness and Quietness of Hospital Environment Baseline Rate
			Cleanliness and Quietness of Hospital Environment Performance Rate
			Cleanliness and Quietness of Hospital Environment Achievement Points
			Cleanliness and Quietness of Hospital Environment Improvement Points
			Cleanliness and Quietness of Hospital Environment Dimension Score
			Discharge Information Floor
			Discharge Information Achievement Threshold
			Discharge Information Benchmark
			Discharge Information Baseline Rate
			Discharge Information Performance Rate
			Discharge Information Achievement Points
			Discharge Information Improvement Points
			Discharge Information Dimension Score
			Overall Rating of Hospital Floor
			Overall Rating of Hospital Achievement Threshold
			Overall Rating of Hospital Benchmark
			Overall Rating of Hospital Baseline Rate
			Overall Rating of Hospital Performance Rate
			Overall Rating of Hospital Achievement Points
			Overall Rating of Hospital Improvement Points
			Overall Rating of Hospital Dimension Score
			HCAHPS Base Score
			HCAHPS Consistency Score
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

. Stop hadoop and postgres
	. Stop hive 
		- ps -ef/grep metastore
		- kill <id>
	.`/root/stop-hadoop.sh`
	.`/data/stop_postgres.sh`

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