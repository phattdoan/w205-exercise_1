#!/bin/bash

# save my current directory
MY_CWD=$(pwd)

# remove staging directory
rm ~/staging/exercise_1/*
rmdir ~/staging/exercise_1
rmdir ~/staging


# remove hdfs directory
hdfs dfs -rm /user/w205/hospital_compare/hospital/hospitals.csv
hdfs dfs -rmdir /user/w205/hospital_compare/hospitals
hdfs dfs -rm /user/w205/hospital_compare/complications/complications.csv
hdfs dfs -rmdir /user/w205/hospital_compare/complications
hdfs dfs -rm /user/w205/hospital_compare/hais/hais.csv
hdfs dfs -rmdir /user/w205/hospital_compare/hais
hdfs dfs -rm /user/w205/hospital_compare/returns/returns.csv
hdfs dfs -rmdir /user/w205/hospital_compare/returns
hdfs dfs -rm /user/w205/hospital_compare/care/care.csv
hdfs dfs -rmdir /user/w205/hospital_compare/care
hdfs dfs -rm /user/w205/hospital_compare/measures/measures.csv
hdfs dfs -rmdir /user/w205/hospital_compare/measures
hdfs dfs -rm /user/w205/hospital_compare/surveys/surveys.csv
hdfs dfs -rmdir /user/w205/hospital_compare/surveys

hdfs dfs rm /user/w205/hospital_compare/*
hdfs dfs -rmdir /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit