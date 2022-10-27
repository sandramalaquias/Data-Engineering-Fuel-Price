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

class FileToS3Operator(BaseOperator):
    template_fields = ('link_file','file_name',)

    @apply_defaults
    def __init__(self,
        aws_credentials="",
        bucket_name="",
        link_file="",
        data_type="",
        file_name="",
        link_args={},
        *args, **kwargs):

        super(FileToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.bucket_name=bucket_name
        self.link_file=link_file
        self.data_type=data_type
        self.file_name=file_name


    def execute(self, context):
        logging.info(f"Parameters: {self.aws_credentials}, {self.bucket_name}, {self.link_file}, {self.data_type}, {self.file_name}")
        connection = BaseHook.get_connection(self.aws_credentials)
        secret_key = connection.password # This is a getter that returns the unencrypted pass
        access_key = connection.login # This is a getter that returns the unencrypted login

        bucket = Variable.get(self.bucket_name)
        row = 0

        link = self.link_file
        logging.info(link)

        if self.data_type == 'json':
            df = pd.read_json(link)
            row, col  = df.shape

        elif self.data_type == 'csv':
            df = pd.read_csv(link, sep=";", decimal=",")
            row, col = df.shape
            
        elif self.data_type == 'csv-l':
            df = pd.read_csv(link, sep=";", decimal=",", header=0, encoding='latin-1')
            row, col = df.shape
            
        elif self.data_type == 'csv-h':
            headers = {"User-Agent": "pandas"}
            df = pd.read_csv(link, sep=";", storage_options=headers, decimal=",")
            row, col = df.shape

        elif self.data_type == 'xlxs':
           df = pd.read_excel(link)
           row, col = df.shape

        elif self.data_type == 'xcom':
           dict_data = context['task_instance'].xcom_pull(key=link)
           df = pd.DataFrame.from_dict(dict_data)
           row, col = df.shape

        else:
           raise ValueError(f"Data type {self.data_type} is not in the scope")


        if row == 0:
           logging.error(f"Read file from {link} result in 0 row")
        else:
           logging.info(f"Read file from {link} \n row = {row}")

        file_json = df.to_json(orient="records")

#Creating Session With Boto3
        session = boto3.Session(
          aws_access_key_id=access_key,
          aws_secret_access_key=secret_key,
          region_name='us-west-2'
        )

#Creating S3 Resource From the Session.
        s3 = session.resource('s3')
        s3_object = s3.Object(bucket, self.file_name)
        s3_insert = s3_object.put(Body=file_json)

        result = s3_insert.get('ResponseMetadata')

        if result.get('HTTPStatusCode') == 200:
            logging.info(f'File Uploaded Successfully {self.file_name}')
        else:
            logging.error(f'File Not Uploaded {self.file_name}')
