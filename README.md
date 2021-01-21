# Spark + AWS Data Lake and ETL

## What is Sparkify?

Sparkify, a music streaming startup, wanted to collect logs they have on user activity and song data and centralize them in a database in order to run analytics. This AWS S3 data lake, set up with a star schema, will help them to easily access their data in an intuitive fashion and start getting rich insights into their user base.

## Why this Database and ETL design?

My client Sparkify has moved to a cloud based system and now keeps their big data logs in an AWS S3 bucket. The end goal was to get that raw .json data from their logs into fact and dimenstion tables in a S3 data lake with parquet files. 

## Database structure overview

![ER Diagram](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1607614393/Song_ERD.png)
*From Udacity*

## How to run

- Start by cloning this repository
- Install all python requirements from the requirements.txt
- Create an S3 bucket and fill in those details in the etl.py `main()` output_data variable
- Initialize an EMR cluster with Spark
- Fill in the dl_template with your own custom details
- SSH into the EMR cluster and upload your `dl_template.cfg` and `etl.py` files
- Run `spark-submit etl.py` to initialize the spark job and write the resultant tables to parquet files in your s3 output path