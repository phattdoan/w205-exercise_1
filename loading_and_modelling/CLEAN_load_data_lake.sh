#!bin/bash

# save my current directory
MY_CWD = $(pwd)

# remove staging directory
rm ~/staging/exercise_1/*
rm ~/staging/exercise_1
rm ~/staging


# remove hdfs directory
hdfs dfs -rm /user/w205/hospital_compare/hospital.csv
hdfs dfs -rm /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit