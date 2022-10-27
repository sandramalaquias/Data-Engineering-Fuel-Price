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

#---------------------Create Brent petroil rate -----------------------------------  

""" Create Brent rate
This table has a Brent oil barrel price. The measurement is daily (except on weekend).

This function:
    - get row data about brent oil barrel rate from S3
    - Select data from Jan/2007
    - calculate the average for month using simple average 
    - transform row data in data table
    - rename columns name
    - create a Brent table 

Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: Brent table
    
"""
def process_brent_table(spark, input_data, output_data, fileout, writeout):
    print ('====> BRENT')
    
    input_path = input_data + 'brent.json'
    
    #read brent oil and select columns 
    df = spark.read\
        .option("mode", "DROPMALFORMED")\
        .json(input_path)  

    df.createOrReplaceTempView("brent_data")
    
    brent_table = spark.sql\
        (""" with brent_parc as
             (select cast(concat(substring(value.VALDATA,1,4),
                                substring(value.VALDATA,6,2)) 
                    as int) as year_month,  
                    value.VALVALOR as value                  
           from    brent_data   
           where   cast(SUBSTRING(value.VALDATA, 1, 4) as int) > 2006
             and   value.VALVALOR is not null)
             
        select avg(value) as brent_avg_value, 
               cast(substring(year_month,1,4) as int) as brent_year,
               year_month as brent_year_month
        from brent_parc
        group by year_month
        order by year_month
    """)
    
    brent_table.printSchema()
    
    output_path = output_data + 'brent_table'
    print (input_path, output_path)                                                  
    
    if writeout == 'y':
        brent_table_summary = brent_table.coalesce(1)
        if fileout == 'json':
            print ('BRENT json')        
            brent_table_summary.write\
               .partitionBy("brent_year")\
               .mode("overwrite").json(output_path)
        else:
            print ('BRENT parquet')        
            brent_table_summary.write\
               .partitionBy("brent_year")\
               .mode("overwrite").json(output_path)
    else:
        print ("Brent")
        # Get row count
        rows = brent_table.count() 
        print("BRENT Rows count :", rows)
        brent_table.printSchema()
        brent_table.show(5)
        
    return brent_table
               
def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    brent_data = process_brent_table(spark, input_data, output_data, fileout, writeout)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()                                        

