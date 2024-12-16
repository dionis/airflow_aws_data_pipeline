
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago

from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteBucketTaggingOperator,
    S3DeleteObjectsOperator,
    S3FileTransformOperator,
    S3GetBucketTaggingOperator,
    S3ListOperator,
    S3ListPrefixesOperator,
    S3PutBucketTaggingOperator,
)

from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator


import pandas as pd
import os
import requests
import json

EXTERNAL_URL_CSV = Variable.get('EXTERNAL_URL_CSV') if  Variable.get('EXTERNAL_URL_CSV', default_var = None) not in ['', None]  else 'https://www.stats.govt.nz/assets/Uploads/Balance-of-payments/Balance-of-payments-and-international-investment-position-June-2024-quarter/Download-data/balance-of-payments-and-international-investment-position-june-2024-quarter.csv'
DATASET_ADDRESS = Variable.has_key('DATASET_ADDRESS').get('DATASET_ADDRESS') if  Variable.get('DATASET_ADDRESS', default_var = None) not in ['', None] else '/opt/airflow/datasets/'

AWS_S3_STORE_BUCKET_NAME = Variable.get('AWS_S3_STORE_BUCKET_NAME') if  Variable.get('AWS_S3_STORE_BUCKET_NAME', default_var = None) not in ['', None]  else 'geekscastle-challenge'

BASE_FILENAME = 'csv_processing'

URL_NOT_DEFINED = 'Url not defined'

ADDRES_TO_SAVE_NOT_DEFINED = 'Address to save in local directories not defined'

FAILED_DOWNLOAD_CSV = 'Failed to download CSV file. Status code:'

BUCKET_S3_FILE_KEY = 'BUCKET_S3_FILE_KEY'

NEW_NAME_DOWNLOAD_FROM_S3 = 's3_downloaded_file.csv'

SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:694619293848:data_engineer_email_topic"

REDSHIFT_TABLE = 'income'

REDSHIFT_SCHEMA = 'public'

SNS_SUCCESSFUL_MESSAGE = f" Download and Upload processing taks was finalized, copied information on RedSfhit DataBase: {REDSHIFT_SCHEMA} in Table: {REDSHIFT_TABLE}"

FILTER_CSV_FILE_NAME = 'filter_process_file.csv'

FILE_TO_UPLOAD = 'FILE_TO_UPLOAD'

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(
        dag_id = 'airflow_pipeline_challenge',
        default_args=default_args,
        description='',
        start_date = days_ago(2),
        schedule_interval = None,
        catchup=False,
        tags=['']
) as dag:

    start = EmptyOperator(task_id='start')
    
    def get_random_filename(file_name):
         suffix = datetime.now().strftime("%y%m%d_%H%M%S")
         filename = "_".join([file_name, suffix])            
         filename = filename.replace('.csv', '')    
         
         return filename
    
    
    def download_csv_file(url, save_address, file_name, **kwargs):
        try:
            if url is None or url == '':
               raise Exception(URL_NOT_DEFINED)  
            elif save_address == None or save_address == '':
               raise Exception(ADDRES_TO_SAVE_NOT_DEFINED)
         
            response = requests.get(url)
         
            if response.status_code == 200:
               # Save the content of the response to a local CSV file              
               filename = get_random_filename(file_name)
            
               filename = filename.replace('.csv', '')
               file_address = save_address + os.sep + filename + '.csv'
            
               if os.path.exists(file_address):
                  os.remove(file_address)
                
               with open(file_address, "wb") as f:
                f.write(response.content)
                    
               print("CSV file downloaded successfully")
            else:
               print(FAILED_DOWNLOAD_CSV, response.status_code)

            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(file_address)   
                
            kwargs['ti'].xcom_push(
                    key = f"{FILE_TO_UPLOAD}", 
                    value = [ filename + '.csv'],
                )  
          
        except:
            raise Exception("Cannot read CSV file")
    
    def upload_to_aws_s3(data_folder,gcs_path,**kwargs):
        data_folder = data_folder
        bucket_name = AWS_S3_STORE_BUCKET_NAME# Your GCS bucket name
        aws_conn_id = 'aws_default'

        if not (os.path.exists(data_folder)):
            print(f"Not  exits address in {data_folder}")
        else:
            csv_files = kwargs['ti'].xcom_pull( key = 'FILE_TO_UPLOAD')
            
            if  csv_files is None or len(csv_files) == 0:
                csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv') ]

            
            for csv_file in csv_files:
                local_file_path = os.path.join(data_folder, csv_file)


                gcs_file_path =  f"{gcs_path}/{csv_file}" if gcs_path.strip() != '' else f"{csv_file}"

                print(f"Copy files from {local_file_path} to {gcs_file_path}...  ")

            

                #######################################################################
                #
                # The LocalFilesystemToGCSOperator is an Airflow operator designed specifically 
                # for uploading files from a local filesystem to a GCS bucket.
                #
                ###################################################################

                create_local_to_s3_job = LocalFilesystemToS3Operator(
                     task_id  =  "create_local_to_s3_job",
                     filename =  local_file_path,
                     dest_key =  csv_file,
                     dest_bucket = bucket_name,
                     replace = True,
                   )
                 
                #Save the filename store in AQ
                create_local_to_s3_job.execute( context = kwargs)   
                kwargs['ti'].xcom_push(
                    key = f"{BUCKET_S3_FILE_KEY}", 
                    value = [csv_file],
                ) 
                
                
    def download_from_s3(key: str, bucket_name: str, local_path: str, **kwargs) -> str:
        key = key if key != '' else kwargs['ti'].xcom_pull( key = 'BUCKET_S3_FILE_KEY')[0]
        hook = S3Hook()
        file_name = hook.download_file(key = key, bucket_name = bucket_name, local_path = local_path)

        return file_name
    
    #
    # S3Hook downloads a file to the local_path folder and gives it an arbitrary name without any extension. 
    # We don’t want that, so we’ll declare another task that renames the file.
    #
    #
    
    def rename_s3_download_file(ti, new_name: str, **kwargs) -> None:
        downloaded_file_name = ti.xcom_pull(task_ids = ['download_from_s3_task'])
        downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
        os.rename(src = downloaded_file_name[0], dst = f"{downloaded_file_path}/{new_name}")
        
   
    def process_and_filter_download_file():
       
        file_path = DATASET_ADDRESS + os.sep + NEW_NAME_DOWNLOAD_FROM_S3
       
        if os.path.exists(file_path): #If exist file
           
           #Transform to PandasData frame and filter
           df = pd.read_csv(file_path)
           
           df["Period_Year"] = pd.to_datetime(df["Period"], format = "%Y.%m").dt.strftime('%Y')
           
           df_filter = df[df['Period_Year'].between('2005', '2020')]   
           
           df_filter = df[df['Data_value'] >= 200]
           
           print(f"Dataset size {df.size} and Filter Dataset {df_filter.size}")
           
           new_filter_name =  FILTER_CSV_FILE_NAME
           
           new_filter_file_path = DATASET_ADDRESS + os.sep + new_filter_name
           
           df_filter.to_csv(new_filter_file_path, encoding='utf-8', index=False)
           
           kwargs['ti'].xcom_push(
                    key = f"{FILE_TO_UPLOAD}", 
                    value = [new_filter_name],
                ) 
           
           #Upload to  S3           
           upload_to_aws_s3(DATASET_ADDRESS, AWS_S3_STORE_BUCKET_NAME)

           return new_filter_file_path 
                   
      
       #Save to RedShift directly or save to S3
    
    
    process_and_filter_download_file_task = PythonOperator(
        task_id='process_and_filter_download_file_task',
        python_callable = process_and_filter_download_file,
    )
   
    rename_s3_download_file_task = PythonOperator(
        task_id='rename_s3_download_file_task',
        python_callable = rename_s3_download_file,
        op_kwargs = {
            'new_name': NEW_NAME_DOWNLOAD_FROM_S3
        }
    )
   
    download_from_s3_task = PythonOperator(
        task_id='download_from_s3_task',
        python_callable = download_from_s3,
        op_kwargs={
            'key': '',
            'bucket_name': AWS_S3_STORE_BUCKET_NAME,
            'local_path': DATASET_ADDRESS
        }
    )
    
    download_csv_task = PythonOperator(
        task_id="download_csv_task",
        python_callable = download_csv_file,
        op_args = [EXTERNAL_URL_CSV, DATASET_ADDRESS, BASE_FILENAME],
    )
    
    upload_to_aws_s3 = PythonOperator(
        task_id = 'upload_to_aws_s3',
        python_callable = upload_to_aws_s3,
        op_args = [DATASET_ADDRESS, AWS_S3_STORE_BUCKET_NAME],
        provide_context = True,       
     ) 
    
    
    s3_to_redshift_task = S3ToRedshiftOperator(
        task_id='s3_to_redshift_task',
        schema = REDSHIFT_SCHEMA,
        table = REDSHIFT_TABLE,
        s3_bucket = AWS_S3_STORE_BUCKET_NAME,
        s3_key = {FILTER_CSV_FILE_NAME},
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default',
        copy_options=[
            "FORMAT AS CSV DELIMITER ',' QUOTE '\"' IGNOREHEADER 1 "
        ],
        method='REPLACE'
    )
    
    sns_publish_notified_task = SnsPublishOperator(
        task_id = 'sns_publish_notified_task',
        target_arn = SNS_TOPIC_ARN,
        message = SNS_SUCCESSFUL_MESSAGE,
)
    
    end = EmptyOperator(task_id='end')

    start >>  download_csv_task >> upload_to_aws_s3 >> download_from_s3_task >> rename_s3_download_file_task >> process_and_filter_download_file_task >> s3_to_redshift_task >> end >> sns_publish_notified_task
    
