
import pandas as pd
import logging

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import task, get_current_context
from operators import FuelToS3Operator

from airflow.models import Variable
from airflow.hooks.base import BaseHook

import json
import boto3

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'smm',
    'start_date': datetime(2007, 2, 1, 0, 0, 0, tzinfo=local_tz),
    'end_date': datetime(2022, 1, 16, 0, 0, 0, tzinfo=local_tz),
    'depends_on_past': False,
    'catchup':False,
    'retries': 3,
   }

dag = DAG('fuel_hist',
    description='Load historical fuel to S3 using Airflow(new_fuel.py)',
    default_args=default_args,
    schedule_interval="0 0 15 */6 *", 
    tags=["smm"] 
   )
        
def get_link(*args, **kwargs):
        context = get_current_context()
        ts = datetime.fromisoformat(context['ts'])
        year = int(datetime.strftime(ts, '%Y'))
        month = int(datetime.strftime(ts, '%m'))
        
        if month   >  6:
           semester = '01'
        else:
           semester = '02'
           year -= 1

        link1 = f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-{semester}.csv"
        file_name = f"myproject/fuel_hist/{year}/semester/{semester}/fuel.json"
        data_link={"link": link1, "file_name": file_name}
            
        ti = kwargs['ti']
        ti.xcom_push(key='link', value=data_link) 
        logging.info(f"Link to file from Get_link: {file_name}")
        
get_link = PythonOperator(
     task_id = 'create_link',
     python_callable = get_link,
     provide_context = True,
     dag = dag
     )
     
create_fuel= FuelToS3Operator(
        task_id="fuel_hist",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
         )

get_link >> create_fuel

