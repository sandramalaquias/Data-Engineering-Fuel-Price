## file_to_s3.py  => operator

import logging
import pandas as pd
import json
import boto3

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from airflow.operators.python import get_current_context


"""The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.
The operator creates and runs a SQL COPY statement based on the parameters provided.
The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file.
Another important requirement of the stage operator is containing a templated field that allows it to load
timestamped files from S3 based on the execution time and run backfills."""

class FuelToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self,
        aws_credentials="",
        bucket_name="",
        *args, **kwargs):

        super(FuelToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.bucket_name=bucket_name

    def execute(self, context):
        logging.info(f"Parameters: {self.aws_credentials}, {self.bucket_name}")
                
        #get link to download data and file name to upload data
        link = context['task_instance'].xcom_pull(task_ids='create_link', key='link')
                
        logging.info(f"Link ===> {link}")
        
        # read fuel data
        link_fuel = link['link']
        logging.info(f"========> Link: {link_fuel}")
        
        headers = {"User-Agent": "pandas"}
        df_fuel = pd.read_csv(link_fuel, sep=";", storage_options=headers, decimal=',', encoding='latin-1')
        logging.info(f"----- fuel ---- shape {df_fuel.shape}")
        
        #get year_month from date
        df_fuel['dates'] = pd.to_datetime(df_fuel['Data da Coleta'], format='%d/%m/%Y')
        df_fuel['year_month'] = pd.to_datetime(df_fuel['dates']).dt.to_period('M').astype(str)
        
        new_name = {'Estado - Sigla': 'UF', 'Valor de Venda': 'Venda', 'Valor de Compra': 'Compra'}
        df_fuel.rename(columns=new_name, inplace=True)
        
        #select only columns needed
        df_sel = df_fuel[["UF",'Municipio','year_month', 'Produto', 'Compra', 'Venda']]
        
        #get mean from prices (sales and purchase)
        df_mean = df_sel.groupby(['UF', 'Municipio', 'year_month', 'Produto']).mean().reset_index()
        
        file_json = df_mean.to_json(orient="records")
        logging.info(f"======> final fuel dataset ----- shape {df_mean.shape}")
        
        #get bucket name
        bucket = Variable.get(self.bucket_name)
        file_name = link['file_name']
                
        ## get aws credentials        
        aws_credentials="my_credentials"
        connection = BaseHook.get_connection(aws_credentials)
        secret_key = connection.password # This is a getter that returns the unencrypted pass
        access_key = connection.login # This is a getter that returns the unencrypted login
    
        #start connection with AWS
        session = boto3.Session(
          aws_access_key_id=access_key,
          aws_secret_access_key=secret_key,
          region_name='us-west-2'
        )
        
        #load fuel data into S3
        
        s3 = session.resource('s3')
        s3_object = s3.Object(bucket, file_name)
        s3_insert = s3_object.put(Body=file_json)

        result = s3_insert.get('ResponseMetadata')

        if result.get('HTTPStatusCode') == 200:
           logging.info(f'File Uploaded Successfully {file_name}')
        else:
           logging.error(f'File Not Uploaded {file_name}')
        






        
