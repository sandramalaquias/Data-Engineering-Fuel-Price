## ## load_dim.py   => operator

import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.base import BaseHook 

"""The LoadFactorOperator is expected to be able to load any fact_table to Amazon Redshift. 
The operator creates and runs a SQL statement based on the parameters provided. 
"""

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    sql_insert = """insert into {} ({})"""
    sql_delete = """delete {};"""

    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials="",
        table="",
        sql_load_table="", 
        delete_table="",       
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.sql_load_table=sql_load_table
        self.delete_table=delete_table      
                   
    def execute(self, context):
        connection = BaseHook.get_connection(self.aws_credentials)
        secret_key = connection.password # This is a getter that returns the unencrypted pass   
        access_key = connection.login # This is a getter that returns the unencrypted login 
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        logging.info("Loading table {self.table}")
        
        if self.delete_table == True:
           formatted_sql = LoadDimensionOperator.sql_delete.format(self.table)
           redshift_hook.run(formatted_sql)
        
        formatted_sql = LoadDimensionOperator.sql_insert.format(self.table,self.sql_load_table)
        
        print ("sql_load_table", formatted_sql)
            
        redshift_hook.run(formatted_sql)  


        

