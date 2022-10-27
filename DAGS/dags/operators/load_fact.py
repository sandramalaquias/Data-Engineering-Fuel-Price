## ## load_fact.py   => operator

import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.base import BaseHook 

"""The LoadFactorOperator is expected to be able to load any fact_table to Amazon Redshift. 
The operator creates and runs a SQL statement based on the parameters provided. 
"""

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    
    sql_insert = """insert into {} ({})"""

    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials="",
        table="",
        sql_load_table="",        
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.sql_load_table=sql_load_table        
                   
    def execute(self, context):
        connection = BaseHook.get_connection(self.aws_credentials)
        secret_key = connection.password # This is a getter that returns the unencrypted pass   
        access_key = connection.login # This is a getter that returns the unencrypted login 
        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_sql = LoadFactOperator.sql_insert.format(self.table,self.sql_load_table)
        
        print ("sql_load_table", formatted_sql)
            
        redshift_hook.run(formatted_sql)  
        
        logging.info(f"Loading fact table {self.table}")


        

