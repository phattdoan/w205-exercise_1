#!bin/bash

# save my current directory
MY_CWD = $(pwd)

# create staging directory
mkdir ~/staging
mkdir ~/staging/exercise_1

# change to staging directory
cd ~staging/exercise_1

# get medicare data file
$MY_URL = "https://data.medicare.gov/views/bg9k-emty/files/4a66c672-a92a-4ced-82a2-033c28581a90?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip"
wget "$MY_URL" -O medicare_data.zip

# unzip the medicare data
unzip medicare_data.zip

# remove first line of files and rename

# create hdfs directory
hdfs dfs -mkdir /user/w205/hospital_compare

#-- Hospital Information
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#-- 
#"Timeline and Effective Care - Hospital.csv" - procedure data
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"Readmissions and Deaths - Hospital.csv" - procedure data
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"Measure Dates.csv" - mapping of measures to codes
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"hvbp_hca_hps_05_28_2015.csv" - survey response data
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"COMPLICATIONS-HOSPITAL.csv" - complications
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"HEALTHCARE ASSOCIATED INFECTIONS-HOSPITALS.csv"
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"PAYMENT-HOSPITAL.csv" - payment and value of care
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

#"MEDICARE HOSPITAL SPENDING PER PATIENT-HOSPITAL.csv" - medicare spending 
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit