import get_path as GP
import get_credentials as GC 
import city_data       as CD
import spark_session as SS

import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


from pyspark.sql.functions import udf, col, trim, count, to_timestamp, when, expr, upper

from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, to_timestamp, expr, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,DoubleType,\
                 LongType, ArrayType
from pyspark.sql.functions import from_json, col
#---------------------Create Fuel price at gas stations table -----------------------------------  

""" Create a table to value of fuel sold at gas stations
The measurement is weekly but some transformation was done in Airflow.

This function:
    - get data about Fuel value from S3 that is transformed in Airflow
    - Select data from Jan/2007
    - transform transformed data in data table
    - rename columns name
    - gather de information from historical and actual measurement
    - create a complete fuel table
    
Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: Fuel table
    
"""
def process_fuel_table(spark, input_data, output_data, fileout, writeout, cities):
    print ("===> Fuel")
                       
    input_path = input_data + 'fuel_hist/*/*/*/fuel.json' 
    
    df_hist = spark.read.option("recursiveFileLookup", "true")\
                .option("mode", "DROPMALFORMED")\
                .json(input_path)\
                .withColumnRenamed("Compra", "fuel_purchase_price")\
                .withColumnRenamed('Municipio', 'fuel_city_name')\
                .withColumnRenamed("Produto", "fuel_product_name")\
                .withColumnRenamed("UF", "fuel_state_code")\
                .withColumnRenamed("Venda", "fuel_sales_price")\
                .withColumnRenamed("year_month", "fuel_date")\
                .withColumnRenamed("Profit %", "fuel_profit_percent")\
               
    input_path = input_data + 'fuel_atual/*/*/fuel.json'   
    
    df_atual = spark.read.option("recursiveFileLookup", "true")\
                .option("mode", "DROPMALFORMED")\
                .json(input_path)\
                .withColumnRenamed("Compra", "fuel_purchase_price")\
                .withColumnRenamed('Municipio', 'fuel_city_name')\
                .withColumnRenamed("Produto", "fuel_product_name")\
                .withColumnRenamed("UF", "fuel_state_code")\
                .withColumnRenamed("Venda", "fuel_sales_price")\
                .withColumnRenamed("year_month", "fuel_date")\
                .withColumnRenamed("Profit %", "fuel_profit_percent")
                
        
# join historical data with actual data
    df = df_hist.union(df_atual)
    
    #get year_month 
    df.createOrReplaceTempView("fuel_table")
    df_fuel = spark.sql(""" select fuel_city_name, fuel_state_code,        fuel_product_name, 
                                   fuel_purchase_price, fuel_sales_price,   
                                   fuel_product_name                       as fuel_product,
                                   cast(concat(substring(fuel_date,1,4),
                                        substring(fuel_date,6,2)) as int)  as fuel_year_month,
                                   cast(substring(fuel_date,1,4) as int)   as fuel_year
                        
              from fuel_table
              where substring(fuel_date,1,4) > '2006'
              order by fuel_product_name, fuel_year_month
    """)
     
#join city to get the city_code (IBGE)
    cities.createOrReplaceTempView("city_data")
    df_fuel.createOrReplaceTempView("fuel_data")
                       
    fuel_table=spark.sql(""" 
    select fuel_state_code,     city.city_code as fuel_city_code,       fuel_product_name, 
           fuel_purchase_price, fuel_sales_price,    fuel_product,      fuel_year_month, fuel_year
           
    from fuel_data
    join city_data as city on fuel_city_name = city.city and fuel_state_code = city.city_state_code
    """)      
    
    output_path = output_data + 'fuel_table'
    if writeout == 'y':
        fuel_table_summary = fuel_table.coalesce(1)
        if fileout == 'json':
            print ('Fuel json')        
            fuel_table_summary.write\
                .partitionBy("fuel_product", "fuel_year")\
                .mode("overwrite").json(output_path)
        else:
            print ('Fuel parquet')        
            fuel_table_summary.write\
                .partitionBy("fuel_product", "fuel_year")\
                .mode("overwrite").parquet(output_path)
    else:
        print ("Fuel")
        # Get row count
        rows = fuel_table.count() 
        print("FUEL Rows count :", rows)
        fuel_table.printSchema()
        fuel_table.show(5)

    return fuel_table
                       
def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    write_city = 'n'
    city_data   = CD.process_city_table(spark, input_data, output_data, fileout, write_city)
    fuel_data = process_fuel_table(spark, input_data, output_data, fileout, writeout, city_data)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()                
