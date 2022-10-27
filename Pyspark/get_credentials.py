import configparser
#-----------------GET AWS Credentials  ------------------------------------------------
    
""" This function get AWS credential from configpath 
    If the configpath is unacessable, return empty values
    
    Input:  config_path
    Output: Return AWS Credentials (ACCESS, SECRET, TOKEN) or empty values
    
"""

def get_credentials(config_path):
    print ('get_credentials')
    config = configparser.ConfigParser()

    try:
        with open(config_path) as credentials:
            config.read_file(credentials)
            return config['my-aws']['aws_access_key_id'],\
                   config['my-aws']['aws_secret_access_key']
          
    except IOError:
        print ('config was not informed')
        return "", ""
    
def main():
    aws_key, aws_secret = get_credentials('/usr/spark-2.4.1/data/aws.cfg')
    print (aws_key)
    
    
if __name__ == "__main__": 
    main()
    
