import findspark
findspark.init()

import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import get_credentials, get_path


#-----------------Working with Spark Session-------------------------------------------------------

""" The function "create_spark_session" creates a spark session using AWS credentials if config file was informed
    In sparkConf, set enviroment to access AWS S3 using TemporaryCredentials if TOKEN Credential is provided
    To read s3a, neeed to enable S3V4 to JavaOptions
    In this case was needed to use: SparkConf, sparkContex and SparkSession to flag all AWS enviroment
    
    Input: aws credencials (access_key, secret_key, token)
           localrun => identify the type of run 
           
    Output: Return spark session as spark
"""
    
def spark_start(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_TOKEN, localrun):
    print ("====> Start spark")
    
    conf = SparkConf()
    conf.setAppName('pyspark_aws')
    conf.set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    conf.set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
    conf.set('spark.driver.memory', '15g')
    
    ## configure AWS Credentials
    if AWS_TOKEN:
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
        conf.set('spark.hadoop.fs.s3a.session.token', AWS_TOKEN)
     
    if AWS_ACCESS_KEY:        
        conf.set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        conf.set('spark.hadoop.fs.s3a.endpoint', 's3-us-west-2.amazonaws.com')
        
    if localrun == 'y':
        conf.setMaster('local[*]')
        
    if AWS_SECRET_KEY:    
        conf.set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
    
    
    #print(conf.toDebugString())

    sc = SparkContext(conf=conf)
    sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

    spark = SparkSession \
            .builder \
            .appName('pyspark_aws') \
            .getOrCreate()

    print('session created')

    return spark 

## stop spark
def spark_stop(spark):
    print ("====> Stop spark")
    spark.sparkContext.stop()
    spark.stop()
    
    
def main():
    input_data, output_data, localmode, config_data = get_path.get_path()
    aws_key, aws_secret = get_credentials.get_credentials(config_data)
    spark = spark_start(aws_key, aws_secret, "", localmode)
    spark_stop(spark)
    
    
if __name__ == "__main__": 
    main()
    