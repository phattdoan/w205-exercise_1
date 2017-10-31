### Phat Doan
### w205 Section 2 - Fall 2017
### Exercise 1

**Question to answer**
1/ What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
2/ What states are models of high-quality care?
3/ Which procedures have the greatest variability between hospitals?
4/ Are average scores for hospital quality or procedural variability correlated with patient survey responses?


## Phase 1: Setting up the data lake
### Setting up environment:
. Start AWS
. Attached volume & start HDFS + Postgres
	Find the volume location: `fdisk -l`
	   . Mount the volume as follows: `mount -t ext4 /dev/xvdf /data`
	   . Start HDFS, Hadoop Yarn and Hive: `/root/start-hadoop.sh`
	   . Start Postgres: `/data/start_postgres.sh`
	   . Start Hive metastore
	   	- /data/start_metastore.sh
	   	- /data/spark15/bin/spark-sql

. switch to `su w205`

. Run git_load_remove.sh to clone the git repo for execution of loading_and_modelling

. Load data with:
	- `cd w205-exercise_1/loading_and_modelling`
	- `./load_data_lake.sh` to download CMS dataset and load necessary files into HDFS
	- If loading fails, run this to clean the data lake before troubleshooting `./CLEAN_load_data_lake.sh`
	
. Shut down procedures:
	. Stop hive 
		- ps -ef|grep metastore
		- kill <id>
	.`/root/stop-hadoop.sh`
	.`/data/stop_postgres.sh`

### Identify key metrics for hospital and state evaluation as the models of high-quality care:
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
. Potential problems with the data:
	- This dataset is from CMS, thus the data might be skewed as it represents a Medicare population rather than a truly generalized American population


## Phase 2: 
### Exploring the dataset: 
`select hospital_type, count(*) from hospitals group by hospital_type;`
`select state, count(*) from hospitals where hospital_type like '%Acute%' 
group by state order by state;`
. There are 3 types of hospitals observed from the data:
	Childrens: 99
	Acute Care hospitals: 3369
	Critical Access hospitals: 1344 - critical access hospital is a designation given to certain rural hospitals by the Centers for Medicare and Medicaid Services (CMS)

`select emergency_services, count(*) from hospital_info group by emergency_services;`
. With emergency services
	No      238
	Yes     3131

. With meets criteria:
	Not Available   1
	Y       3075
	        55
For this review purpose, I would focus on Acute Care hospitals with emergency services and meet CM<S criteria since childrens and critical access hospitals serve different purposes and certainly would require different measuring metrics./

### transforming data lake
`source transforming/hospital_info.sql`
. hospital_info: contain information and CMS-aggregated measures for acute care hospitals with emergency services only, excluding childrens and critical access hospitals.

`source transforming/hospital_compare.sql`
. hospital_compare:
	provider_id,
	hospital_overall_rating,
	mortality_national_comparison,
	safety_of_care_national_comparison,
	readmission_national_comparison,
	patient_experience_national_comparison,
	effectiveness_of_care_national_comparison,
	timeliness_of_care_national_comparison,
	efficient_use_of_medical_imaging_national_comparison

`source transforming/hospital_baseline.sql`
. hospital_baseline: for each CMS measure and its comparison to national, each comparison can be 'above', 'below' or 'no different than the national average'. Using this, I transform the comparison into a score, with 3 for 'above national average', 1 for 'below national average', and 2 for 'no different' or null variable. This allows me to aggregate the score later into one combined metric for measuring the performance of each hospital.
	provider_id,
	mortality_score,
	safety_score,
	readmission_score,
	patient_experience_score,
	effectiveness_score,
	timeliness_score,
	efficiency_score

`source transforming/states_hospital_count.sql`
. states_hospital_count: count number of hospitals per state
	state
	hospital_count


## Phase 3: investigating
### Question 1: Top hospitals in the nation using CMS metrics
`source top_hospital.sql`
. Using hospital_baseline with CMS-aggregated metrics, these are the top 10 hospitals in the country:
	CITIZENS MEDICAL CENTER VICTORIA        TX      Voluntary non-profit - Other    20
	SHANNON MEDICAL CENTER  SAN ANGELO      TX      Voluntary non-profit - Private  19
	ADVENTIST HINSDALE HOSPITAL     HINSDALE        IL      Voluntary non-profit - Church   19
	RIVERSIDE MEDICAL CENTER        KANKAKEE        IL      Voluntary non-profit - Private  19
	AVERA ST LUKES  ABERDEEN        SD      Voluntary non-profit - Church   19
	MERCY HOSPITAL  IOWA CITY       IA      Voluntary non-profit - Church   19
	FAIRVIEW HOSPITAL       CLEVELAND       OH      Voluntary non-profit - Private  19
	SCOTTSDALE THOMPSON PEAK MEDICAL CENTER SCOTTSDALE      AZ      Proprietary     19
	SHERMAN HOSPITAL        ELGIN   IL      Voluntary non-profit - Private  19
	AVERA HEART HOSPITAL OF SOUTH DAKOTA    SIOUX FALLS     SD      Proprietary     19


### Question 2: What states are models of high-quality care?
`source transforming\states_hospital_count.sql`
`select * from states_hospital_count`
States that include on acute care hospitals, not surprisingly most hospitals concentrated in states with dense population
	AK      8
	AL      81
	AR      42
	AS      1
	AZ      51
	CA      256
	CO      46
	CT      27
	DC      7
	DE      6
	FL      165
	GA      95
	GU      1
	HI      12
	IA      34
	ID      11
	IL      118
	IN      75
	KS      41
	KY      64
	LA      71
	MA      55
	MD      43
	ME      17
	MI      82
	MN      44
	MO      70
	MP      1
	MS      55
	MT      12
	NC      82
	ND      8
	NE      18
	NH      12
	NJ      62
	NM      29
	NV      19
	NY      143
	OH      114
	OK      77
	OR      34
	PA      134
	PR      46
	RI      11
	SC      51
	SD      16
	TN      87
	TX      280
	UT      31
	VA      73
	VI      2
	VT      6
	WA      48
	WI      63
	WV      28
	WY      10

To determine the states that are models of high-quality care, I calculate the percentage of hospitals in the top 50 for each state. 
`source investigating/top_hospitals.sql`
Top 5 states with highest percentage of hospitals in the top 50 based on the rating:
SD      0.125
KS      0.09756097
IN      0.093333334
HI      0.083333336
IL      0.076271184
MO      0.071428575


### Question 3: Which procedures have the greatest variability between hospitals?
Using data from care_transformed table, I estimate the variance of each procedure with measure_id grouping and score. The measures and scores are selected based on actual scores and excluded those measures with step metrics (i.e. above, below, etc.). I only inlcude measures that have more than 30 entries to avoid small sample bias.
The top 10 procedures that have the highest variance are:
`source transforming/care_transformed.sql`
`source investigating/hospital_variability.sql`
ED_2b	Admit Decision Time to ED Departure Time for Admitted Patients
OP_18b	Median Time from ED Arrival to ED Departure for Discharged ED Patients
ED_1b   Median Time from ED Arrival to ED Departure for Admitted ED Patients
OP_31	Percentage of patients aged 18 years and older who had cataract surgery and had improvement in visual function achieved within 90 days following the cataract surgery
OP_23	Head CT or MRI Scan Results for Acute Ischemic Stroke or Hemorrhagic Stroke Patients who Received Head CT or MRI Scan Interpretation 
OP_3b	Median Time to Transfer to Another Facility for Acute Coronary Intervention- Reporting Rate
OP_29	Appropriate Follow-Up Interval for Normal Colonoscopy in Average Risk Patients
OP_30	Colonoscopy Interval for Patients with a History of Adenomatous Polyps - Avoidance of Inappropriate Use
OP_2	Fibrinolytic Therapy Received Within 30 Minutes of ED Arrival
OP_20	Median Time from ED Arrival to Provider Contact for ED patients

### Question 4: Are average scores for hospital quality or procedural variability correlated with patient survey responses?
`source transforming/surveys_transformed.sql`
`source investigating/hospitals_patients_corr.sql`
I compute the correlation coefficient between hospital score and patients survey responses using top_hospitals and hospital_baseline tables.

Correlation between hospital score and survey rating:
Nurse rating = 0.5340670295160149       
Doctor rating = 0.4434510774602345     
Staff rating = 0.5186374103477489     
Pain management = 0.48148498316864735    
Quality communication = 0.450553145164991      
Environment = 0.44895231097870686       
Quality discharge = 0.44964214024464566 
Overall hospital performance = 0.563389188296042

Overall, hospital score by CMS correlate somewhat with patients survey rating.
Particularly, staff rating and hospital rating is the highest, followed by environment rating, patient rating of the hospital,doctor rating, nurse rating, pain management, pain medicine communication, and quality discharge.
