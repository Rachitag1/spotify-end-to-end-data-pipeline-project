# Spotify End-to-End Data Pipeline Project

### Introduction
Built an ETL (Extract, Transform, Load) pipeline to extract the data of Top 50 Global songs from Spotify API on AWS, transform, load and analyze the same for valuable insights. 

### Project Overview

🔸 Data processing pipeline steps:

1. Data Extraction from the Spotify API:
   First lambda function (spotify_api_data_extract) was created to extract the information from Spotify API in JSON format and uploads it to an AWS S3 bucket.The extraction of data is 
   enabled by a trigger on a desired frequency.
2. Data Transformation:
   A second lambda function(spotify_transformation_load_function.py) was created which gets triggered each time the data/a new element is uploaded in the S3 bucket.This script takes the 
   raw data stored in s3 and pulls the information for album, artist, and song, converts it to 'csv' format and uploads the transformed data to S3 bucket in the respective folders.
3. Data Schema:
   Three crawlers are created on AWS Glue Crawler, one for each 'csv' file. These crawlers are used to read the 'csv' files for album, artist and song and create the table schema for 
   each entity.This also allows the creation of three tables that are then available for analysis on Amazon Athena.
4. Data Analysis: On Amazon Athena, it's possible to run various SQL queries to analyze the information obtained.

🔸 Tools Used 

- Spotify API 
- Python 
- Amazon Lambda 
- Amazon Simple Storage Service (S3) 
- AWS Glue Crawler 
- Amazon Athena



