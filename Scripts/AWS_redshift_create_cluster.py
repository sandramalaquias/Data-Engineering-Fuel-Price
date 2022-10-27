import pandas as pd
import boto3
import json
import configparser
import sql


""" STEP 0: Create your AWS user and get the key and scret key
             Update the file DWH1 - fields identifyied as "update"
"""

""" Step 1: Load DWH / S3 Params from a file
    return: config and s3 param to acess the boto3 resources
"""

def get_configfile():
    print ("----------------1---------------")
    print ("1 Get AWS config")
   
    config = configparser.ConfigParser()
    config.read_file(open('dwh1.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    ## acess s3 data
    S3_UDACITY_REGION      = config.get("S3", "UDACITY_REGION")
    S3_UDACITY_BUCKET      = config.get("S3", "UDACITY_BUCKET")
    S3_LOG_DATA_FILTER     = config.get("S3", "LOG_DATA_FILTER")
    S3_SONG_DATA_FILTER    = config.get("S3", "SONG_DATA_FILTER")
    S3_LOG_JSONPATH_FILTER = config.get("S3", "LOG_JSONPATH_FILTER")
    S3_MYBUCKET            = config.get("S3", "MY_S3")

    
    ## DWH to dataframe

    dwh_list = ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", \
                "DWH_DB", "DWH_DB_USER",  \
                "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"]

    dwh_value = [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
                 DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
               
    dwh_db = pd.DataFrame({"Param": dwh_list, "Value": dwh_value })

    dwh_db.set_index('Param', inplace=True)

    
    ## S3 to dataframe

    s3_list = ["S3_UDACITY_REGION", "S3_UDACITY_BUCKET", "S3_LOG_DATA_FILTER", "S3_SONG_DATA_FILTER", \
               "S3_LOG_JSONPATH_FILTER", "S3_MYBUCKET"]

    s3_value = [S3_UDACITY_REGION, S3_UDACITY_BUCKET, S3_LOG_DATA_FILTER, S3_SONG_DATA_FILTER, S3_LOG_JSONPATH_FILTER, 
                S3_MYBUCKET]

               
    s3_db = pd.DataFrame({"Param": s3_list, "Value": s3_value})
    s3_db.set_index('Param')
    
    #print dwh param
    print ("-----------------------------------")
    print ("1.1 - DWH Param")
    print (dwh_db.to_markdown(), "\n")

    print ("1.2 - S3 Param")
    print (s3_db.to_markdown()) 
       
    ## get boto3 param
    iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )
    s3 = boto3.resource('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    redshift = boto3.client('redshift',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    ec2 = boto3.resource('ec2',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    s3cMyb = boto3.client('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    s3Myb = boto3.resource('s3',
                       region_name=S3_UDACITY_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    boto3_param = [iam, s3, redshift, ec2, s3cMyb, s3Myb]
    
    ## return config param
    return config, boto3_param


""" -----------------------------------------------------------------------
STEP 2: Create a Rule to Readshif Acess S3 bucket - read only 
        This function receive a Config file and boto3_param (acess AWS resources)
"""

def get_iam_arn(config, boto3_param):
    print ("----------------2---------------")
    print ("2 Create IAM Rule")
    
    DWH_IAM_ROLE_NAME      = config.get('DWH', 'DWH_IAM_ROLE_NAME')
    iam = boto3_param[0]
    
# create IAM Role
    from botocore.exceptions import ClientError
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
    )    
    except Exception as e:
        print("2.1 Creating a new IAM Role:", DWH_IAM_ROLE_NAME, "- Error=", e)
    else:
        print("2.1 Created a new IAM Role:", DWH_IAM_ROLE_NAME)
    
# Attaching Policy to access S3

    try:
        response = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    
    except Exception as e:
        print("2.2 Attaching Policy: AmazonS3ReadOnlyAccess - Error=", e)
                                          
    else:
        response = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
        print("2.2 Attached Policy: AmazonS3ReadOnlyAccess - Status Code:", response['ResponseMetadata']['HTTPStatusCode'], \
               'Date:', response['ResponseMetadata']['HTTPHeaders']['date'])
        print('    Effect:', response['Role']['AssumeRolePolicyDocument']['Statement'][0]['Effect'], "\n")
    
    ## Attaching rules for Redshift commands
    try:   
        response = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
               PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
                                     )
    except Exception as e:
        print("- Error=", e)
    else:
        print ("2.3 Attached Policy: AmazonRedshiftAllCommandsFullAccess - Status Code:", response['ResponseMetadata']['HTTPStatusCode'], 
        'Date:', response['ResponseMetadata']['HTTPHeaders']['date'])
    
    response = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    roleArn = response['Role']['Arn']

    print('2.4 ', roleArn, 'Effect:', response['Role']['AssumeRolePolicyDocument']['Statement'][0]['Effect'])

    #Get the ARN section
    ARN = config["ARN"]

    #Update ARN
    ARN["arn_rule"] = roleArn
    
    #Write changes back to config file
    with open('dwh1.cfg', 'w') as configfile:
        config.write(configfile)
    
    print ("2.5 Config file Arn updated")
    
"""--------------------------------------------------------------------    
   Step 3- Acess S3 Udacity Bucket
   This function receive a config file and boto3_param (access AWS resources)
   Return a list of files
"""

## get list of file from S3 Udacity Bucket
def list_obj(filter_arg, sampleDbBucket): 
    list_file = []
    
    for obj in sampleDbBucket.objects.filter(Prefix=filter_arg):
        list_file.append(obj.key)
        
    return list_file
    
# Access S3 Udacity
def acess_s3_udacity(config, boto3_param):
   
    print ("----------------3---------------")
    print ("3 Udacity S3") 
    
    ##get param
    
    S3_UDACITY_REGION      = config.get('S3', 'UDACITY_REGION')
    S3_UDACITY_BUCKET      = config.get('S3', 'UDACITY_BUCKET')
    S3_SONG_DATA_FILTER    = config.get('S3', 'SONG_DATA_FILTER')
    S3_LOG_DATA_FILTER     = config.get('S3', 'LOG_DATA_FILTER')
    S3_LOG_JSONPATH_FILTER = config.get('S3', 'LOG_JSONPATH_FILTER')
    
    s3 = boto3_param[1]

    sampleDbBucket =  s3.Bucket(S3_UDACITY_BUCKET)

    ## Print sample of file names

    # LOG_DATA
    print ("3.1 - Log Data")
    log_file = list_obj(S3_LOG_DATA_FILTER, sampleDbBucket)

    ## SONG_DATA
    print ("3.2 - Song Data")
    song_file = list_obj(S3_SONG_DATA_FILTER, sampleDbBucket)

    ## JSON PATH
    print ("3.3 - Json Log Data")
    json_file = list_obj(S3_LOG_JSONPATH_FILTER, sampleDbBucket)
 
    files_udacity = [log_file, song_file, json_file]
    return files_udacity
    
""" ----------------------------------------------------------------------------------
    STEP 4 create my_Bucket, attach policy public and load file song manifest
           This function receive config, boto3_paran and list of udacity_files
           files_udacity = [log_file, song_file, json_file]
"""

# create my bucket 
def create_mybucket(config, boto3_param, files_udacity):
    print ("----------------4---------------")
    print ("4 Create MyBuckect and load Manifest Song data")
    
    s3cMyb = boto3_param[4]
    S3_UDACITY_REGION      = config.get("S3", "UDACITY_REGION")
    S3_MYBUCKET            = config.get("S3", "MY_S3")
    S3_UDACITY_BUCKET      = config.get('S3', 'UDACITY_BUCKET')
    
    location = {'LocationConstraint': S3_UDACITY_REGION}
    
    try:
        s3cMyb.create_bucket(Bucket=S3_MYBUCKET, 
                    CreateBucketConfiguration=location)
    except Exception as e:
        print ('4.1 Error in create bucket', S3_MYBUCKET, e)
    else:
        print ('4.1 Created bucket', S3_MYBUCKET)
    
    # Create a bucket policy - public

    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'AddPerm',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': ['s3:GetObject'],
            'Resource': f'arn:aws:s3:::{S3_MYBUCKET}/*'
        }]
    }

    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)

    # Set the new policy
    try:
        s3cMyb.put_bucket_policy(Bucket=S3_MYBUCKET, Policy=bucket_policy)
    except Exceptio as e:
        print ('4.2 Error in set policy: s3:GetObject', e)    
    else:
        print ('4.2 Set policy: s3:GetObject') 
    
    ##create a manifest file for song
    song_data = files_udacity[1]
    a = 's3://' + S3_UDACITY_BUCKET + '/'
    song_s3 = [''.join([a, x]) for x in song_data]
    song_s3.pop(0)
 
    manifest = {}
    manifest['entries'] = []

    for x in song_s3:
        manifest['entries'].append({"url": x})
    
    manifest_file = json.dumps(manifest)

    ## upload manifest file to my bucket
    s3Myb = boto3_param[5]                    

    s3Myb_obj = s3Myb.Object(S3_MYBUCKET, 'song.manifest')

    try:
        result = s3Myb_obj.put(Body=manifest_file)
    except Exception as e:
        print ('4.3 Error in load song manifest', e)
    else:
        print ('4.3 Song Manifest file loaded')
    

""" -------------------------------------------------------------
STEP 5:  Create Redshift Cluster  
        This function receive config and boto3_param
"""
              
## create cluster
def create_cluster(config, boto3_param):
    print ("----------------4---------------")
    print ("5 Create Redshift Cluster")
    
    ##get param     
    DWH_CLUSTER_TYPE       = config.get('DWH','DWH_CLUSTER_TYPE')
    DWH_NODE_TYPE          = config.get('DWH','DWH_NODE_TYPE')
    DWH_NUM_NODES          = config.get('DWH','DWH_NUM_NODES')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_DB                 = config.get('DWH', 'DWH_DB')
    DWH_DB_USER            = config.get('DWH','DWH_DB_USER')
    DWH_DB_PASSWORD        = config.get('DWH','DWH_DB_PASSWORD')
    ARN_ROLE               = config.get('ARN',"ARN_RULE")

    redshift = boto3_param[2]                                   
    dict_clusters = {}
    dict_clusters['ClusterStatus'] = None
                    
    try:
        response = redshift.create_cluster(        
            #add parameters for HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            ## add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[ARN_ROLE]  
        )
    except Exception as e:
        print("5.1 Error in create redshift cluster:", e)
    else:  
        myClusters = redshift.describe_clusters()['Clusters']
        print ("5.1", dict_clusters['ClusterStatus'])
    

    ### Waiting until Cluster available """  
    print ("5.2 Waiting until cluster available")
    no_available = True

    while no_available:
        myClusters = redshift.describe_clusters()['Clusters']
        if myClusters == []:
            print ("5.2 There are no cluster")
        else:
            dict_clusters = myClusters[0]
            if dict_clusters['ClusterStatus'] == 'available':
                no_available = False
            
    print ("5.3 Cluster", dict_clusters['ClusterStatus'])
    
    ### Print cluster details and get Endpoint"""
 
    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in myClusterProps.items() if k in keysToShow]
    cluster_db = pd.DataFrame(data=x, columns=["Key", "Value"])
    print ("5.4 Cluster details")
    print (cluster_db.to_markdown())
    
    ## get endpoint and ARN and Update config - will be used to get DB connect
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address'] 
    DWH_VPC = myClusterProps['VpcId']
    
    #Get the ARN section
    ARN = config["ARN"]

    #Update ARN
    ARN["arn_endpoint"] = DWH_ENDPOINT
    ARN['arn_vpcID'] = DWH_VPC                        

    #Write changes back to config file
    with open('dwh1.cfg', 'w') as configfile:
        config.write(configfile)
    
    print ("5.5 Config ENDPOINT/VPC file updated")
    
    #  Open an incoming  TCP port to access the cluster ednpoint
  
    ARN_VPC                = config.get("ARN", "ARN_VPCID")
    DWH_PORT               = config.get('DWH', 'DWH_PORT')
    
    ec2 = boto3_param[3]
 
    try:
        vpc = ec2.Vpc(ARN_VPC)
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    except Exception as e:
            print("5.6 TCP port acess Error", e)
    else:
           print("5.6 TCP port acesss:", defaultSg)  
           
        
    ### STEP7 - conect to Redshift data (Amazon Redshift is based on PostgreSQ 
      
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
    print("5.7 Redshift Postgree Connected")

    #Get the Postgree section
    POST = config["POSTGREE"]

    #Update string connection
    POST["postgree_connect"] = conn_string

    #Write changes back to config file
    with open('dwh1.cfg', 'w') as configfile:
        config.write(configfile)
    
    print ("5.8 Config file Reshift Connect updated")
    
    #Records information about transactions that currently hold locks on tables in the database
    print ("5.9 Redshift Transactions information")
    
    df_svv = pd.read_sql('select * from svv_transactions', conn_string)
    print (df_svv.to_markdown())
    
""" === Main === """
def main():
    ## get configuration and Key
    config, boto3_param = get_configfile()
    
    ## create IAM rule
    ##get_iam_arn(config, boto3_param)
    
    ## acess Udacity S3 Bucket 
    ##list_file = acess_s3_udacity(config, boto3_param)
    
    ## load Manifest to song
    ##create_mybucket(config, boto3_param, list_file)
    
    ## create redshift cluster and get connection
    create_cluster(config, boto3_param)
    
    print ("------------- End AWS Connect -------------")
    
if __name__ == "__main__":
    main()