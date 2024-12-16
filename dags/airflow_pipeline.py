
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
    
    
    def download_csv_file(url, save_address, file_name, **kwargs):
        try:
            if url is None or url == '':
               raise Exception(URL_NOT_DEFINED)  
            elif save_address == None or save_address == '':
               raise Exception(ADDRES_TO_SAVE_NOT_DEFINED)
         
            response = requests.get(url)
         
            if response.status_code == 200:
               # Save the content of the response to a local CSV file
               suffix = datetime.now().strftime("%y%m%d_%H%M%S")
               filename = "_".join([file_name, suffix])
            
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
          
        except:
            raise Exception("Cannot read CSV file")
    
    def upload_to_aws_s3(data_folder,gcs_path,**kwargs):
        data_folder = data_folder
        bucket_name = AWS_S3_STORE_BUCKET_NAME# Your GCS bucket name
        aws_conn_id = 'aws_default'

        # List all CSV files in the data folder
        # Note : you can filter the files extentions with file.endswith('.csv')
        # Examples : file.endswith('.csv')
        #            file.endswith('.json')
        #            file.endswith('.csv','json')

        #print(f"File location using __file__ variable: {os.path.realpath(os.path.dirname(__file__))}")      


        if not (os.path.exists(data_folder)):
            print(f"Not  exits address in {data_folder}")
        else:
            csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv') or file.endswith('.json') ]

            
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
                
               #  with open(local_file_path, 'rb') as data: 
               #    create_object = S3CreateObjectOperator(
               #          task_id="s3_create_object",
               #          s3_bucket = bucket_name,
               #          s3_key= csv_file,
               #          data = data,
               #          replace=True,
               #       )
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
        
    task_rename_s3_download_file = PythonOperator(
        task_id='task_rename_s3_download_file',
        python_callable = rename_s3_download_file,
        op_kwargs = {
            'new_name': 's3_downloaded_file.csv'
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
        # op_kwargs: Optional[Dict] = None,
        op_args = [EXTERNAL_URL_CSV, DATASET_ADDRESS, BASE_FILENAME],
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
    
    upload_to_aws_s3 = PythonOperator(
        task_id = 'upload_to_aws_s3',
        python_callable = upload_to_aws_s3,
        op_args = [DATASET_ADDRESS, AWS_S3_STORE_BUCKET_NAME],
        provide_context = True,       
     )
    
    setup__task_create_table = RedshiftSQLOperator(
        task_id='setup__create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )
    
    task_insert_data = RedshiftSQLOperator(
        task_id='task_insert_data',
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
    )
    
    task_get_all_table_data = RedshiftSQLOperator(
        task_id='task_get_all_table_data', sql="CREATE TABLE more_fruit AS SELECT * FROM fruit;"
    )
    
    
    task_get_with_filter = RedshiftSQLOperator(
        task_id='task_get_with_filter',
        sql="CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={'color': 'Red'},
    )
    
    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        schema='public',
        table='income',
        s3_bucket = AWS_S3_STORE_BUCKET_NAME,
        s3_key='csv_processing_241214_222149.csv',
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default',
        copy_options=[
            "FORMAT AS CSV DELIMITER ',' QUOTE '\"' IGNOREHEADER 1 "
        ],
        method='REPLACE'
    )
    
    end = EmptyOperator(task_id='end')

    start >> download_csv_task >> upload_to_aws_s3 >> download_from_s3_task >> task_rename_s3_download_file >>  setup__task_create_table >> task_insert_data >> task_get_all_table_data >> task_get_with_filter >> s3_to_redshift >> end
    
    #  