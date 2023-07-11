import yaml
import boto3
from pyspark.sql import SparkSession

data = yaml.safe_load(open('s3config.yaml'))
# print(data['access_key'])

# credential = {
#     'username' : 'username',
#     'password' : 'password',
#     'users' : ['user1', 'user2', 'user3']
# }
#
# yaml.dump(credential, open('s3config.yaml', 'a'))
s3 = boto3.client('s3', aws_access_key_id=data['data']['access_key'],
                  aws_secret_access_key=data['data']['secret_access_key'], verify=False)

data_file = s3.get_object(Bucket=data['data']['bucket_name'],
                          Key=data['data']['file_key'])

bucket_name = data['data']['bucket_name']
key = data['data']['file_key']
file_name = key.split('/')[1]
s3.download_file(bucket_name,key, './yaml_dwn/'+file_name)
print('Downloaded.....')
