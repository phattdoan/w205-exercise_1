## Phat Doan
## w205 Section 2 - Fall 2017
## Exercise 1

**Question to answer**
1/ What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
2/ What states are models of high-quality care?
3/ Which procedures have the greatest variability between hospitals?
4/ Are average scores for hospital quality or procedural variability correlated with patient survey responses?


## Week 1:
Files:
. Load data_lake: https://raw.githubusercontent.com/phattdoan/UCB-205-HDFS-HIVE-SPARK/master/load_data_lake.sh

Plan:
. Understand the database
	- Hospital
	- Readmission rate
	- Patient satisfaction
	- Cost of care
. Build schema 
. Load dataset into Hadoop

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

. Get the dataset:

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
tail -n +2 "$MY_FILE" > hospitals.css


Hadoop loading:
hdfs dfs -ls /user/w205
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put hospitals.csv /user/w205/hospital_compare


QA:
check exercise1 directory
chec