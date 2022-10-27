## get the information from spark-submit terminal
import get_path        as GT

## get AWS credentials
import get_credentials as GC

## create spark session
import spark_session   as SS

## ETL to city_data - that is data about Brazil municipality)
import city_data       as CD

## ETL to IPCA data - that is data about time line of brasilian inflation
import ipca_data       as ID

## ETL to USD data - that is about time line of Dolar rate
import usd_data        as UD

## ETL to brent data - that is about time line of price of brut petroil oil
import brent_data      as BD

## ETL to Wage data - that is about time line of Brazil wage minimum
import wage_data       as WD

## ETL to fuel data - that is about time line of fuel price
import fuel_data       as FD

## creaate manifest fiels to load data into Redshift
import manifest_file   as MF
import sys

""" Example about spark-submit 
spark-submit test.py --inputpath s3a://smmbucket/myproject/ --outputpath s3a://smmbucket/myproject/output/ --configpath //usr/spark-2.4.1/data/aws.cfg --localrun y --fileout json --writeout s
"""

def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GT.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    spark = SS.spark_start(aws_key, aws_secret, "", localmode)
    city_data   = CD.process_city_table(spark, input_data, output_data, fileout, writeout)
    ipca_data   = ID.process_ipca_table(spark, input_data, output_data, fileout, writeout)
    usd_data    = UD.process_usd_table(spark, input_data, output_data, fileout, writeout)
    brent_data  = BD.process_brent_table(spark, input_data, output_data, fileout, writeout)
    wage_data  =  WD.process_wage_table(spark, input_data, output_data, fileout, writeout)
    fuel_data =   FD.process_fuel_table(spark, input_data, output_data, fileout, writeout, city_data)
    MF(aws_key, aws_secret, output_data)
    SS.spark_stop(spark)
    
if __name__ == "__main__": 
    main()
    