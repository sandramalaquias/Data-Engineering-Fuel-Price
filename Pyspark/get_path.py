import configparser
import argparse

import sys
#---------------------Get Input/output/config path -----------------------------------

""" This process get input/output/config path as param from spark-submit
    All arguments are optional
    inputpath  => standard path for read data
    outputpath => standard path to write data
    configpath => path for get credentials. This is ".cfg" type
    localRun   => Y/N => param to spark
    fileout    => type of outputfile like json or parquet 
    writeout   => write out files in S3 (Y/N) 
    
    If input/output/config/local is not informed, will working using
        Input_path = 's3a://udacity-dend/"
        Output_path = 's3a://smmbucketudacity" (this is my bucket)
        Configpath  = has no defaul value
        localRun    = "n"
        fileout     = "json"
        writeout    = 'y'
        
    Examples:
    - via spark:
        spark-submit get_path.py --inputpath s3a://smmbucket/myproject/ --outputpath s3a://smmbucket/myproject/output/ 
        --configpath /usr/spark-2.4.1/credentials --localrun y --fileout json  --writeout y

        To get help: spark-submit get_path.py --help
    
"""
    
def get_path():
# Initiate the parser
    print ("====>  get path")

    parser = argparse.ArgumentParser(description='Path for input/output/config files')

    # Add long and short argument
    parser.add_argument("--inputpath", "-inputpath", help="The URI for input bucket location like s3a://smmbucket")
    parser.add_argument("--outputpath", "-outputpath", help="The URI for output S3 bucket location like s3a://smmbucket/myproject/output/")
    parser.add_argument("--configpath", "-configtpath", help="The URI for your AWS config file like /usr/spark-2.4.1/credentials")
    parser.add_argument("--localrun", "-localrun", help="N=default (NO), Y=run in local mode (yes)")
    parser.add_argument("--fileout", "-fileout", help="Type of output file. Default: Json")
    parser.add_argument("--writeout", "-writeout", help="N=default (NO), Y=write files (yes) - default 'Y'")
   
    # Read arguments from the command line
    args = parser.parse_args()

    # Check for --inputpath
    if args.inputpath:
        inputpath=args.inputpath
    else:
        inputpath = 's3a://smmbucket/myproject/'
        
    # Check for --outputpath
    if args.outputpath:
        outputpath = args.outputpath
    else:
        outputpath ='s3a://smmbucket/myproject/output/'
        
    if not args.localrun or not (args.localrun.lower() == 'y' or args.localrun.lower() == 'n'):
        localrun = 'n'
    else:
        localrun = args.localrun.lower()        
        
    if not args.fileout or not (args.fileout.lower() == 'json' or args.fileout.lower() == 'parquet'):
        fileout = 'json'
    else:
        fileout = args.fileout.lower()        
        
    if not args.writeout or not(args.writeout.lower() == 'y' or args.writeout.lower() == 'n'):
        writeout = 'y'
    else:
        writeout = args.writeout.lower()
    
    if not args.configpath:
        config_data = '/usr/spark-2.4.1/data/aws.cfg'
    else:
        config_data = args.configpath
        
    print (inputpath, outputpath, config_data, localrun, fileout, writeout)
    print ('-----------------------------------------')
    return inputpath, outputpath, config_data, localrun, fileout, writeout


#-------------
def main():
    input_data, output_data, config_data, localrun, fileout, writeout = get_path()
    print (get_path())
    
if __name__ == "__main__": 
    if 'ipykernel' in sys.modules:
        ip = 'notebook'
        input_data = 's3a://smmbucket/myproject/'
        output_data = 's3a://smmbucket/myproject/output/'
        localmode = 'y'
        config_data = '/usr/spark-2.4.1/credentials'

    elif 'IPython' in sys.modules: 
        ip = 'iPython'
        print (get_path())
  
    main()