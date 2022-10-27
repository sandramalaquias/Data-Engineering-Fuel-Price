## ## data_quality   => operator

import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.base import BaseHook 


class DataQualityOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials="",
        tables=[], 
        sql_count="",
        sql_result=0,    
        *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id    = redshift_conn_id
        self.aws_credentials     = aws_credentials
        self.tables              = tables
        self.sql_count           = sql_count
        self.sql_result          = sql_result
                           
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)                
        logging.info (f"sql quality test: {self.sql_count}, result expect: {self.sql_result}")
        count_error = False
        
        for check_table in self.tables:
            sql_sel = self.sql_count.format(check_table['column'], check_table['table'], check_table['column'])
            logging.info(sql_sel)
            records = redshift_hook.get_records(sql_sel)          
            num_records = records[0][0]
            if num_records > self.sql_result:
               count_error=True
               logging.info(f"Data quality check failed. {check_table['table']} table contained {num_records} records with null values in column {check_table['column']}")
            else:
               logging.info(f"Data quality check was sucessfull. {check_table['table']} table contained {num_records} records with null values in column {check_table['column']}")
              
        if count_error:
           raise ValueError(f"Data quality check failed.")
        
        
        
        
       
        



        

