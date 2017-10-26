#!bin/bash

# save my current directory
MY_CWD = $(pwd)

# remove staging directory
rm ~/staging/exercise_1/*
rm ~/staging/exercise_1
rm ~/staging


# remove hdfs directory
hdfs dfs -rm /user/w205/hospital_compare/hospital/hospitals.csv
hdfs dfs -rm /user/w205/hospital_compare/hospital
hdfs dfs -rm /user/w205/hospital_compare/complications/complications.csv
hdfs dfs -rm /user/w205/hospital_compare/complications
hdfs dfs -rm /user/w205/hospital_compare/hais/hais.csv
hdfs dfs -rm /user/w205/hospital_compare/hais
hdfs dfs -rm /user/w205/hospital_compare/returns/returns.csv
hdfs dfs -rm /user/w205/hospital_compare/returns
hdfs dfs -rm /user/w205/hospital_compare/care/care.csv
hdfs dfs -rm /user/w205/hospital_compare/care
hdfs dfs -rm /user/w205/hospital_compare/measures/measures.csv
hdfs dfs -rm /user/w205/hospital_compare/measures
hdfs dfs -rm /user/w205/hospital_compare/surveys/surveys.csv
hdfs dfs -rm /user/w205/hospital_compare/surveys
hdfs dfs -rm /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit