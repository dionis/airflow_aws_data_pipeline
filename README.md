# Project Description:

  Pipeline data using Apache Airflow

#### Request:

- Design a data pipeline using Apache Airflow or AWS Step Functions:
  - Step 1: Download a file from a provided URL.
  - Step 2: Process the file (similar to processing in Part 2).
  - Step 3: Upload the processed results to an S3 bucket.
  - Step 4: Notify via SNS upon completion.

#### Evaluation Criteria:
 - Clear and modular pipeline design.
 - Implementation of dependencies between steps.
 - Clear documentation of the pipeline.


### Data Pipeline Design

 I create an Airflow workflow (DAG) with 11 task. Dag definition and associated Tasks in `dags/airflow_pipeline.py` file. 

  ![image](doc/image/dag_workflow.png)
  - start: First process init the pipeline.d
  - download_csv_task: Download de csv file from url.
  - download_csv_task: Upload to S3 bucket the csv file.
  - download_from_s3_task: Download from S3 bucket the csv file.
  - rename_s3_download_file_task: Rename the csv file downloaded.
  - process_and_filter_download_file_task: Apply a filter data to csv informations.
  - upload_to_aws_s3_filter_file_task: Upload the csv filter file to S3 bucket.
  - s3_to_redshift_task: Copy the csv file in S3 Bucket to RedShift database.
  - sns_publish_notified_task: Clean unused files.
  - end: Last task during pipeline workflow.
  - sns_publish_notified_task: Execute notification about end pipeline process.

### Install and deployment description:

1. Use an Airflow docker images for deploy the pipeline. For more information [check](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1. It is necessary a AWS user and obtain the access key:  

   - AWS_SECRET_ACCES_KEY
   - AWS_ACCESS_KEY_ID
   - AWS_DEFAULT_REGION

    Get the IAM user values associated and copy in docker-compose.yaml environment variables

1. Was created a S3 bucket for same user in AWS. You must create a S3 Bucket and defined the airflow variable AWS_S3_STORE_BUCKET_NAME.

   ![image](doc/image/s3_bucket.png)

1. Was downloaded a [csv file from url](https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-June-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2024-quarter.csv), but you can modified the url address defined the Airflow variable `EXTERNAL_URL_CSV`.   However you must modified the filter csv task named `process_and_filter_download_file_task` in dags/airflow_pipeline.py file with csv columns and filter criteria. If you modified the `EXTERNAL_URL_CSV`  another csv file or other type of data, we are not responsable of result or error. 

1. It's importan too, create a aws_default conection with your `AWS_SECRET_ACCES_KEY` and 
`AWS_ACCESS_KEY_ID` with the same user values that docker-compuse.yaml  variable values. 
For more information you can [check this information](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html). 

   ![image](doc/image/airflow_conections.png)



1. Docker images deployment:
   In the repository directory `airflow_aws_data_pipeline` you must execute:
      
      `docker compose up` 

   It's important check before, docker-compose.yml file present.

1. We create a RedSfhit database with a cluster configuration. In the database defined a 
table named income with the same column features than [csv file](https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-June-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2024-quarter.csv).
You can get the table DDL in [table_ddl/income_table.sql](files/table_ddl.sql).  It's a requeriment the table name `income`, with public schema. 

      ![image](doc/image/redshift_cluster_database_table.png)
1. For Airflow conection with the RedShift database you need create a **redshift_default** conection with your Redshift cluster information. For more information you cant [check this information](https://www.astronomer.io/docs/learn/airflow-redshift/). Also, you need configure the RedShift cluster database for permit external conection. For more information [check in post](https://docs.bigeye.com/docs/redshift).

1. Was created an Amazon Simple Notification Service (Amazon SNS) notificator for send email, if you want 
received notification when workflow (pipeline) finalized, contact to developer team.

    ![image](doc/image/sns_aws_topic.png)

