
## udac_project_dag.py

from datetime import datetime, timedelta
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators import FileToS3Operator

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'smm',
    'start_date': datetime.now(),
    'schedule_interval': '@monthly', 
    'depends_on_past': False,
    'catchup':False,
   }

dag = DAG('file_to_s3',
          default_args=default_args,
          description='Load data into S3 Airflow',
          tags=["smm"]
        ) 
            
def exchange_rate_link(*args, **kwargs):
    link_file="https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40moeda=%27{}%27&%40dataInicial=%27{}%27&%40dataFinalCotacao=%27{}%27&%24format=json"
    link_args={'moeda': 'USD', 'dataInicial': datetime.strftime(datetime.now() - timedelta(days=7300), '%m-%d-%Y'), 'dataFinal': datetime.strftime(datetime.now() - timedelta(days=15), '%m-%d-%Y')}
    link = link_file.format(*link_args.values())
    return link
          
                       
create_ipca_file= FileToS3Operator(
        task_id="ipca_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
        link_file='https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/63/p/all/d/v63%202',
        data_type="json",
        file_name="myproject/ipca.json"
         )
         
create_exchange_rate= FileToS3Operator(
        task_id="exchange_rate_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
        link_file=exchange_rate_link(),
        data_type="json",
        file_name="myproject/rateUSD.json"
         )
        
create_brent_file= FileToS3Operator(
        task_id="brent_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
        link_file="http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='EIA366_PBRENT366')",
        data_type="json",
        file_name="myproject/brent.json" 
         )
         
create_city_file= FileToS3Operator(
        task_id="city_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
        link_file="http://blog.mds.gov.br/redesuas/wp-content/uploads/2018/06/Lista-de-Munic%C3%ADpios-com-IBGE-Brasil.xlsx",
        data_type="xlxs",
        file_name="myproject/city.json"   
         )
         
create_wage_file= FileToS3Operator(
        task_id="wage_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="s3_mybucket",
        link_file="http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='MTE12_SALMIN12')",
        data_type="json",
        file_name="myproject/wage.json"    
         )
              
create_ipca_file
create_exchange_rate
create_brent_file
create_city_file
create_wage_file


