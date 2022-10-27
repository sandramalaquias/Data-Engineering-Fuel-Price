

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from operators import FuelToRedshiftOperator, HasRowsOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator

from helpers import SqlCreateTablesMyproject

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'smm',
    'start_date': datetime.now(),
    'schedule_interval':None, 
    'depends_on_past': False,
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('FuelToRedshift',
          default_args=default_args,
          description='Copy manifest file to Redshift using Airflow',
          tags=["smm"]
        ) 
         
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)
       
####  create staging tables

group_create_task = TaskGroup(
    "create_table_task", 
    tooltip="Tasks for create tables",
    dag=dag
) 

with group_create_task:
    create_schema =  PostgresOperator(
        task_id="create_schema",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_schema_fuel
    )
        
    create_city_table= PostgresOperator(
        task_id="create_city",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_city_table
    )
        
    create_state_table= PostgresOperator(
        task_id="create_state",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_state_table
        )
        
    create_ipca_table= PostgresOperator(
        task_id="create_ipca",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_ipca_table
        )
        
    create_usd_table= PostgresOperator(
        task_id="create_usd",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_usd_table
        )
        
    create_brent_table= PostgresOperator(
        task_id="create_brent",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_brent_table
        )
    create_wage_table= PostgresOperator(
        task_id="create_wage",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_wage_table
        )
        
    create_fuel_table= PostgresOperator(
        task_id="create_fuel",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlCreateTablesMyproject.create_fuel_table
        )


    create_schema >> [create_city_table, create_state_table, create_ipca_table, create_usd_table, create_brent_table,
                      create_wage_table, create_fuel_table]
    
## load data from S3 to Redshift staging

group_load_table = TaskGroup(
    "load_table_task", 
    tooltip="Tasks for load tables",
    dag=dag
) 

with group_load_table:
    city_to_redshift = FuelToRedshiftOperator(
        task_id='load_city',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.city",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/city.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_city = HasRowsOperator(
        task_id='check_city',
        dag=dag,
        table="fuel.city",
        redshift_conn_id="redshift"
        )
        
    state_to_redshift = FuelToRedshiftOperator(
        task_id='load_state',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.state",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/state.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_state = HasRowsOperator(
        task_id='check_state',
        dag=dag,
        table="fuel.state",
        redshift_conn_id="redshift"
        )
        
    ipca_to_redshift = FuelToRedshiftOperator(
        task_id='load_ipca',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.ipca",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/ipca.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_ipca = HasRowsOperator(
        task_id='check_ipca',
        dag=dag,
        table="fuel.ipca",
        redshift_conn_id="redshift"
        )
        
    usd_to_redshift = FuelToRedshiftOperator(
        task_id='load_usd',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.usd",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/usd.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_usd = HasRowsOperator(
        task_id='check_usd',
        dag=dag,
        table="fuel.usd",
        redshift_conn_id="redshift"
        )
        
    brent_to_redshift = FuelToRedshiftOperator(
        task_id='load_brent',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.brent",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/brent.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_brent = HasRowsOperator(
        task_id='check_brent',
        dag=dag,
        table="fuel.brent",
        redshift_conn_id="redshift"
        )
        
    wage_to_redshift = FuelToRedshiftOperator(
        task_id='load_wage',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.wage",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/wage.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_wage = HasRowsOperator(
        task_id='check_wage',
        dag=dag,
        table="fuel.wage",
        redshift_conn_id="redshift"
        )
        
    fuel_to_redshift = FuelToRedshiftOperator(
        task_id='load_fuel',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials="my_credentials",
        table="fuel.fuel",
        bucket_name="s3_mybucket",
        bucket_file="myproject/output/manifest/fuel.manifest",
        schema = 'fuel',
        region = 'us-west-2',
        delete = True
)
    check_fuel = HasRowsOperator(
        task_id='check_fuel',
        dag=dag,
        table="fuel.fuel",
        redshift_conn_id="redshift"
        )
    city_to_redshift >> check_city
    state_to_redshift >> check_state 
    ipca_to_redshift >> check_ipca  
    usd_to_redshift >> check_usd 
    brent_to_redshift >> check_brent 
    wage_to_redshift >> check_wage
    fuel_to_redshift >> check_fuel

# quality test

group_quality = TaskGroup(
    "quality_table_task", 
    tooltip="Tasks for qualitiy",
    dag=dag
) 

with group_quality:

    quality_city = DataQualityOperator(
    task_id='quality_city',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.city', 'column': 'city_code'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_state = DataQualityOperator(
    task_id='quality_state',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.state', 'column': 'state_code'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_ipca = DataQualityOperator(
    task_id='quality_ipca',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.ipca', 'column': 'ipca_year_month'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_usd = DataQualityOperator(
    task_id='quality_usd',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.usd', 'column': 'usd_year_month'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_brent = DataQualityOperator(
    task_id='quality_brent',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.brent', 'column': 'brent_year_month'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_wage = DataQualityOperator(
    task_id='quality_wage',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.wage', 'column': 'wage_year_month'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_fuel = DataQualityOperator(
    task_id='quality_fuel',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="my_credentials",
    tables=[{'table': 'fuel.fuel', 'column': 'fuel_year_month'}], 
    sql_count="""select count({}) as qtd_null from {} where {} is null""",
    sql_result=0
    )
    
    quality_city, quality_state, quality_ipca, quality_usd, quality_brent, quality_wage, quality_fuel

start_operator >> group_create_task >> group_load_table >> group_quality >> end_operator


 
