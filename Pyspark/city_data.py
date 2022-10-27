import get_path as GP
import get_credentials as GC
import spark_session as SS

import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col, trim, count, to_timestamp, when, expr, upper
from pyspark.sql.functions import round as sround

from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, to_timestamp, expr, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,DoubleType,\
                 LongType, ArrayType
from pyspark.sql.functions import from_json, col


#--------------------- Create city and state data -----------------------------------
"""
The city data is about for brazilian municipality and have some information about the cities

This function:
    - transform row data in data table
    - translate some data (from Portuguese to English)
    - create a state table with state name from city data 

Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: none
"""

def process_city_table(spark, input_data, output_data, fileout, writeout):
    print ("===> city table")
    input_path = input_data + 'city.json'
    
    
    df = spark.read\
        .option("mode", "DROPMALFORMED")\
        .json(input_path)\
        .withColumnRenamed("IBGE", "city_code")\
        .withColumnRenamed("IBGE7", 'city_complete_code')\
        .withColumnRenamed('Região', 'city_country_region')\
        .withColumnRenamed("Porte", "city_size")\
        .withColumnRenamed("UF", "city_state_code")\
        .withColumnRenamed("Município", "city_name")\
        .select("city_code", "city_complete_code","city_name",\
                "city_size", "city_state_code", "city_country_region","Capital")
               
               
    df_city = df.withColumn("city_region_code",when(df.city_country_region == "Região Norte","N")\
                                .when(df.city_country_region == "Região Nordeste","NE")\
                                .when(df.city_country_region == "Região Sul", "S")\
                                .when(df.city_country_region == "Região Sudeste", "SE")\
                                .when(df.city_country_region == "Região Centro-Oeste", "CO")\
                                .otherwise(df.city_country_region))\
            .withColumn("city_country_region",when(df.city_country_region == "Região Norte","North")\
                                .when(df.city_country_region == "Região Nordeste","Northeast")\
                                .when(df.city_country_region == "Região Sul", "South")\
                                .when(df.city_country_region == "Região Sudeste", "Southeast")\
                                .when(df.city_country_region == "Região Centro-Oeste", "Midwest")
                                .otherwise(df.city_country_region))\
            .withColumn("city_size",  when(df.city_size == "Pequeno I", "Small I")\
                                .when(df.city_size == "Pequeno II", "Small II")\
                                .when(df.city_size == "Médio", "Midsize")\
                                .when(df.city_size == "Grande", "Large")\
                                .when(df.city_size == "Metrópole", "Metropolis")\
                                .otherwise(df.city_size))\
            .withColumn('city_capital', when(df['Capital'].isNull(),"N").otherwise("Y"))
            
    
    ## round censo_2010
    df_city.createOrReplaceTempView("city_data")
    new_city = spark.sql("""
               select city_state_code, city_code, city_complete_code, city_name, city_size, city_capital,
               city_country_region, 
               city_state_code as UF
               from city_data
               order by city_state_code asc, city_code asc
               """)

 #removing accents from city name
    pd_city = new_city.toPandas()
    pd_city['city'] = pd_city['city_name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    pd_city['city'] = pd_city['city'].str.upper()
    
    
## get back spark dataframe
    
    sp_city = spark.createDataFrame(pd_city)
    sp_city.printSchema()
    
## create state table
    
    df_state = sp_city.filter(sp_city.city_capital == "Y")  
    
    
    df_state = df_state\
        .withColumn('state_name',when(df_state.city_state_code == "AC", "Acre")\
                                .when(df_state.city_state_code == "AL", "Alagoas")\
                                .when(df_state.city_state_code == "AP", 'Amapa')\
                                .when(df_state.city_state_code == "AM", 'Amazonas')\
                                .when(df_state.city_state_code == "BA", 'Bahia')\
                                .when(df_state.city_state_code == "CE", 'Ceara')\
                                .when(df_state.city_state_code == "DF", 'Distrito Federal')\
                                .when(df_state.city_state_code == "ES", 'Espirito Santo')\
                                .when(df_state.city_state_code == "GO", 'Goias')\
                                .when(df_state.city_state_code == "MA", 'Maranhao')\
                                .when(df_state.city_state_code == "MT", 'Mato Grosso')\
                                .when(df_state.city_state_code == "MS", 'Mato Grosso do Sul')\
                                .when(df_state.city_state_code == "MG", 'Minas Gerais')\
                                .when(df_state.city_state_code == "PA", 'Para')\
                                .when(df_state.city_state_code == "PB", 'Paraiba')\
                                .when(df_state.city_state_code == "PR", 'Parana')\
                                .when(df_state.city_state_code == "PE", 'Pernanbuco')\
                                .when(df_state.city_state_code == "PI", 'Piaui')\
                                .when(df_state.city_state_code == "RJ", 'Rio de Janeiro')\
                                .when(df_state.city_state_code == "RN", 'Rio Grande do Norte')\
                                .when(df_state.city_state_code == "RS", 'Rio Grande do Sul')\
                                .when(df_state.city_state_code == "RO", 'Rondonia')\
                                .when(df_state.city_state_code == "RR", 'Roraima')\
                                .when(df_state.city_state_code == "SC", 'Santa Catarina')\
                                .when(df_state.city_state_code == "SP", 'Sao Paulo')\
                                .when(df_state.city_state_code == "SE", 'Sergipe')\
                                .when(df_state.city_state_code == "TO", 'Tocantins')\
                                .otherwise(df_state.city_state_code))
    
    df_state = df_state\
        .withColumnRenamed("city_code", "state_capital_code")\
        .withColumnRenamed("city_complete_code", 'state_capital_complete_code')\
        .withColumnRenamed('city_country_region', 'state_country_region')\
        .withColumnRenamed("city_censo_2010", "state_capital_censo_2010")\
        .withColumnRenamed("city_size", "state_capital_size")\
        .withColumnRenamed("city_state_code", "state_code")\
        .withColumnRenamed("city_name", "state_capital_name")\
        .select("state_code", "state_name", "state_capital_code", "state_capital_complete_code", "state_country_region",\
                "state_capital_name")
    
    df_state.printSchema()
               
    #write city file in S3 (one partition by state)
    
    
    if writeout == 'y':
        df_city_summary  = sp_city.coalesce(1)
        df_state_summary = df_state.coalesce(1)
        
        if fileout == 'json':
            print ('city/state json')
            output_path = output_data + 'city_table'
            df_city_summary.write\
               .partitionBy("UF")\
               .mode("overwrite").json(output_path)
            
            output_path = output_data + 'state_table'
            df_state_summary.write\
               .mode("overwrite").json(output_path)
        else:
            print ('city/state parquet')
            df_city_summary.write\
               .partitionBy("UF")\
               .mode("overwrite").parquet(output_path)
            
            df_state_summary.write\
               .mode("overwrite").parquet(output_path)
    else:
        print ("city")
        # Get row count
        rows = sp_city.count() 
        print("City Rows count :", rows)
        sp_city.printSchema()
        sp_city.show(5)
        
        print("States Rows count :", rows)
        # Get row count
        rows = df_state.count()    
        print("States Rows count :", rows)
        df_state.printSchema()
        df_state.show(5)
        
    return sp_city

def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    city_data = process_city_table(spark, input_data, output_data, fileout, writeout)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()
    