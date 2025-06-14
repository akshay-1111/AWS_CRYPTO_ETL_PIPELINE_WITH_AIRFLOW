# Real-Time Crypto ETL Pipeline

This project shows a real-time ETL pipeline to collect cryptocurrency prices from a public API(coingecko api), process them using AWS services and Apache Spark, and store the data in Snowflake for analytics. The etl is handled by Apache Airflow

## Architecture

1. **Lambda (API Fetch)**
   - Fetches crypto prices from a public API.
   - Triggered by Airflow every 2 minutes.

2. **Kafka (on EC2)**
   - Receives the data pushed by Lambda.

3. **AWS Glue Streaming**
   - Spark streaming job consumes from Kafka.
   - Transforms and writes to S3 in Parquet format.

4. **Snowflake**
   - Snowpipe ingests from S3 to Snowflake table (`my_table`).
   - Airflow task optionally runs a `COPY INTO` if Snowpipe fails.

5. **Airflow**
   - DAG runs the entire orchestration end-to-end.

## Technologies
- AWS Lambda, EC2, Glue Streaming
- Apache Kafka, Spark
- Snowflake + Snowpipe
- Apache Airflow
- Python

## Airflow DAG Tasks
- `trigger_lambda`: invokes Lambda to fetch API data
- `trigger_glue_job`: runs Glue streaming job
- `force_snowpipe_refresh`: refreshes Snowpipe or uses `COPY INTO`

## Prerequisites
- AWS services configured and IAM roles set
- Snowflake account + pipe setup
- Kafka running on EC2

## Project Structure and File Descriptions
- lambda/fetch_api.txt	    =    AWS Lambda function that fetches real-time crypto prices from the API and pushes them to Kafka (on EC2).
- glue/kafka_s3_job.txt     =    AWS Glue Spark Streaming script that reads data from Kafka, processes it, and writes to S3 in Parquet format.
- airflow/dags/dag_etl.py   =	  Airflow DAG that orchestrates the full ETL pipeline: triggers Lambda, runs Glue job, and triggers Snowpipe ingestion or fallback COPY INTO.
- snowflake_worksheet.txt   =    Snowflake SQL commands to create database, schema, table, file format, stage, and pipe are included below.


