import get_path as GP
import get_credentials as GC 
import boto3
import json


#================>  manifest files
""" Create a manifest files to perform load into redshift 

This function:
    - gather all data files 
    - create a manifest file by table
    - write into AWS S3
   
    
Input:
    - Credentials: AWS_ACCESS_KEY, AWS_SECRET_KEY
    
Output: path for output_data

Return: None
    
"""
def create_manifest_files(AWS_ACCESS_KEY, AWS_SECRET_KEY, output_data):
    key = AWS_ACCESS_KEY
    secret = AWS_SECRET_KEY
    
    aws_session = boto3.Session(
          aws_access_key_id=key,
          aws_secret_access_key=secret,
          region_name='us-west-2'
        )
    
    #Creating S3 Resource From the Session.
    s3 = aws_session.resource('s3')

    dict_city  = {"entries":[]}
    dict_state = {"entries":[]}
    dict_ipca  = {"entries":[]}
    dict_usd   = {"entries":[]}
    dict_brent = {"entries":[]}
    dict_wage  = {"entries":[]}
    dict_fuel  = {"entries":[]}
    
    s3_prefix = output_data[16:]
    
    ##get the file name
    print ('=====>', s3_prefix)
    
    dict_count = {'city': 0, 'state': 0, 'ipca': 0, 'usd': 0, 'brent': 0, 'wage': 0, 'fuel':0}
    bucket_name = ''
    
    for bucket in s3.buckets.all():
        for obj in bucket.objects.filter(Prefix=s3_prefix):
            if 'json' in obj.key:
                bucket_name = bucket.name
                file_name = 's3://' + bucket.name + '/' + obj.key
                
                if  "city" in obj.key:
                     dict_city["entries"].append({"url": file_name})
                     dict_count['city'] += 1

                elif  "state" in obj.key:
                    dict_state["entries"].append({"url": file_name})
                    dict_count['state'] += 1            
                                
                elif  "ipca" in obj.key:
                    dict_ipca["entries"].append({"url": file_name})
                    dict_count['ipca'] += 1

                elif  "usd" in obj.key:
                    dict_usd["entries"].append({"url": file_name})
                    dict_count['usd'] += 1

                elif  "brent" in obj.key:
                    dict_brent["entries"].append({"url": file_name})
                    dict_count['brent'] += 1

                elif  "wage" in obj.key:
                    dict_wage["entries"].append({"url": file_name})
                    dict_count['wage'] += 1

                elif  "fuel" in obj.key:
                    dict_fuel["entries"].append({"url": file_name})
                    dict_count['fuel'] += 1
 
    print ('=====>', dict_count)
    
    manifest_city   = json.dumps(dict_city)
    manifest_state  = json.dumps(dict_state)
    manifest_ipca   = json.dumps(dict_ipca)
    manifest_usd    = json.dumps(dict_usd)
    manifest_brent  = json.dumps(dict_brent)
    manifest_wage   = json.dumps(dict_wage)
    manifest_fuel   = json.dumps(dict_fuel)

    ## upload manifest file to bucket 
    manifest_path = output_data[16:] + 'manifest/'
    file_manifest =[manifest_path + 'city.manifest', 
                    manifest_path + 'state.manifest',
                    manifest_path + 'ipca.manifest', 
                    manifest_path + 'usd.manifest',
                    manifest_path + 'brent.manifest', 
                    manifest_path + 'wage.manifest',
                    manifest_path + 'fuel.manifest']  
             
    
    file_body = [manifest_city, manifest_state, manifest_ipca, manifest_usd, manifest_brent,\
                 manifest_wage, manifest_fuel]  
    

    for i in range(len(file_manifest)):
        s3_object = s3.Object(bucket_name, file_manifest[i])

        try:
            budy = file_body[i]
            result = s3_object.put(Body=budy)

        except Exception as e:
            print (e)
                                
                                          
def main():
    input_data, output_data, config_data, localmode, fileout, writeout = GP.get_path()    
    aws_key, aws_secret = GC.get_credentials(config_data)
    create_manifest_files(aws_key, aws_secret, output_data)
    
    
if __name__ == "__main__": 
    main()                                        