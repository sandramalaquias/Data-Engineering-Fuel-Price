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

#---------------------Create Dolar exchange rate -----------------------------------  

""" Create Dolar exchange rate
This table has a exchange rate between USD and Real. The measurement is daily.
For a single day, we have measurement to Open, Intermatiate, and Closed values 
We are using only the closed values (Boletim = "Fechamento")

This function:
    - get row data about dolar exchange rate from S3
    - Select data from Jan/2007
    - calculate the average for month using simple average 
    - transform row data in data table
    - rename columns name
    - create a USD table 

Input:
    spark ==> spark session
    input_data ==>  path to read data
    output_data ==> path to write data table
    
Output: USD table
    
"""

def process_usd_table(spark, input_data, output_data, fileout, writeout):
    print ('====> USD data')
                      
    input_path = input_data + 'rateUSD.json'
    
    #read dolar rate and select columns 
    df = spark.read\
        .option("mode", "DROPMALFORMED")\
        .json(input_path)   
    
     # Get row count
    rows = df.count()
    print("USD rate Rows count :", rows)
     
    df.createOrReplaceTempView("usd_data")
    usd_table = spark.sql\
        (""" with usd_parc as (
            select value.cotacaoCompra as usd_pricePurchase,         
                   value.cotacaoVenda as usd_priceSale,
                   cast(concat(substring(value.dataHoraCotacao,1,4),
                          substring(value.dataHoraCotacao,6,2)) as int) as usd_year_month
           from     usd_data
           where    value.tipoBoletim = "Fechamento")
           
           select avg(usd_pricePurchase) as usd_avg_purchase,
                  avg(usd_priceSale)     as usd_avg_Sale,
                  cast(substring(usd_year_month,1,4) as int) as usd_year,
                  usd_year_month
           from usd_parc 
           where cast(substring(usd_year_month,1,4) as int) > 2006
           group by usd_year_month
           order by usd_year_month
    """)
    
    usd_table.printSchema()
    
    output_path = output_data + 'rateUSD_table'

    if writeout == 'y':
        usd_table_summary = usd_table.coalesce(1)
        if fileout == 'json':
            print ('USD json')        
            usd_table_summary = usd_table.coalesce(1)
            usd_table_summary.write\
               .partitionBy("usd_year")\
               .mode("overwrite").json(output_path)
        else:
            print ('USD parquet')        
            usd_table_summary.write\
               .partitionBy("usd_year")\
               .mode("overwrite").parquet(output_path)
    else:
        print ("USD")
        # Get row count
        rows = usd_table.count() 
        print("USD Rows count :", rows)
        usd_table.printSchema()
        usd_table.show(5)                                                
        
    return usd_table
                      
def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    usd_data = process_usd_table(spark, input_data, output_data, fileout, writeout)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()                                        

