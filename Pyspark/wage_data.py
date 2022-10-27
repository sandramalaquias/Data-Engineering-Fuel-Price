import get_path as GP
import get_credentials as GC 
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


#---------------------Create Wage value table -----------------------------------  

""" Create Wage minimum value
This table has a Wage value to Brazil. The measurement is monthly.

This function:
    - get row data about Wage minimum value from S3
    - Select data from Jan/2007
    - transform row data in data table
    - rename columns name
    - create a Wage table 

Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: Wage table
    
"""
def process_wage_table(spark, input_data, output_data, fileout, writeout):
    print ('===> Wage')
    
    input_path = input_data + 'wage.json'
    
    #read dolar rate and select columns 
    df = spark.read\
        .option("mode", "DROPMALFORMED")\
        .json(input_path)   

    df.createOrReplaceTempView("wage_data")
    
    wage_table = spark.sql\
        ("""
            select value.VALVALOR as wage_value,
            cast(SUBSTRING(value.VALDATA, 1, 4) as int) AS wage_year,
            cast(concat(substring(value.VALDATA,1,4),
                        substring(value.VALDATA,6,2)) as int) as wage_year_month
                                   
           from    wage_data   
           where   cast(SUBSTRING(value.VALDATA, 1, 4) as int) > 2006
             and   value.VALVALOR is not null
           order by SUBSTRING(value.VALDATA, 1, 4)
    """)
    
    output_path = output_data + 'wage_table'
    
    if writeout == 'y':
        wage_table_summary = wage_table.coalesce(1)
        if fileout == 'json':
            print ('Wage json')        
            wage_table_summary.write\
               .partitionBy("wage_year")\
               .mode("overwrite").json(output_path)
        else:
            print ('Wage parquet')        
            wage_table_summary.write\
               .partitionBy("wage_year")\
               .mode("overwrite").json(output_path)
    else:
        print ("Wage")
        # Get row count
        rows = wage_table.count() 
        print("Wage Rows count :", rows)
        wage_table.printSchema()
        wage_table.show(5)

    return wage_table

def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    wage_data = process_wage_table(spark, input_data, output_data, fileout, writeout)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()                                        


