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

#---------------------Create IPCA data (inflation rate) -----------------------------------  
""" Create IPCA data (inflation rate) that has monthly measurement

This function:
    - get row data about IPCA (inflation rate) from S3
    - Select data from Jan/2007
    - transform row data in data table
    - create a IPCA table 
    - Columns of interest:  
        - D1C = '1' - where the data start and has constant value
        - D3C = measure period (format = YYYYMM) 
    
Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: IPCA table
    
"""
def process_ipca_table(spark, input_data, output_data, fileout, writeout):
    print ("===> ipca table")

    input_path = input_data + 'ipca.json'
    

    #read ipca and select columns 
    df = spark.read\
        .option("mode", "DROPMALFORMED")\
        .json(input_path)
    
    df.createOrReplaceTempView("ipca_data")
    ipca_table = spark.sql\
        ("""
        select V as ipca_tax, 
        cast(SUBSTRING(D3C, 1, 4) as int) AS ipca_year,
        cast(D3C as int) as ipca_year_month
           from     ipca_data
           where    D1C == '1' and
                    cast(SUBSTRING(D3C, 1, 4) as int) > 2006
           order by D3C
        """)
    
    output_path = output_data + 'ipca_table'
    
    if writeout == 'y':
        ipca_table_summary = ipca_table.coalesce(1)
        if fileout == 'json':
            print ('IPCA json')        
            ipca_table_summary.write\
               .partitionBy("ipca_year")\
               .mode("overwrite").json(output_path)
        else:
            print ('IPCA parquet')        
            ipca_table_summary.write\
               .partitionBy("ipca_year")\
               .mode("overwrite").parquet(output_path)

    else:
        print ("IPCA")
        # Get row count
        rows = ipca_table.count() 
        print("IPCA Rows count :", rows)
        ipca_table.printSchema()
        ipca_table.show(5)
        
    return ipca_table
                                          
                                          
def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    ipca_data = process_ipca_table(spark, input_data, output_data, fileout, writeout)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()                                        