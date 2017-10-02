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

#remove first line of files and rename
OLD_FILE = "Hospital General Informcation.csv"
NEW_FILE = "hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE

# create hdfs directory
hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit