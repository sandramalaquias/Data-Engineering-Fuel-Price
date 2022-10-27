## fuel_redshift.py  => operator

import logging

from airflow.hooks.postgres_hook import PostgresHook
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

class FuelToRedshiftOperator(BaseOperator):
    
    delete_sql = """ DELETE {};"""
    
    copy_sql = """SET search_path to {}
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        format as json 'auto ignorecase'
        TRUNCATECOLUMNS         
        """  
        
    copy_sql_manifest = ("""SET search_path TO {}; 
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        compupdate off 
        format as json 'auto ignorecase'
        manifest;
        """)


        
    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials="",
        table="",
        bucket_name="",
        bucket_file="",
        schema="",
        region="",
        delete="",
        *args, **kwargs):

        super(FuelToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.bucket_name=bucket_name
        self.bucket_file=bucket_file
        self.schema=schema
        self.region=region
        self.delete=delete
                   
    def execute(self, context):
        execution_date = context['execution_date']
        connection = BaseHook.get_connection(self.aws_credentials)
        secret_key = connection.password # This is a getter that returns the unencrypted pass   
        access_key = connection.login # This is a getter that returns the unencrypted login 
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        bucket = Variable.get(self.bucket_name)
        year=execution_date.year
        month=execution_date.month
        
        
        logging.info(f"Loading stage table {self.table}")
        
        s3_path = f"s3://{bucket}/{self.bucket_file}"
        
        if self.delete:
             formatted_sql = FuelToRedshiftOperator.delete_sql.format(self.table)
             print ("formatted_sql", formatted_sql)     
             redshift_hook.run(formatted_sql)  
        
        if self.bucket_file[-8:] == 'manifest':   
           formatted_sql = FuelToRedshiftOperator.copy_sql_manifest.format(self.schema, self.table, s3_path, access_key, secret_key, self.region)
        else: 
           formatted_sql = FuelToRedshiftOperator.copy_sql.format(self.schema, self.table, s3_path, access_key, secret_key, self.region)           
         
        print ("formatted_sql", formatted_sql)
            
        redshift_hook.run(formatted_sql)  


        

