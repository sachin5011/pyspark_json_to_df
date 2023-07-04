from pyspark.sql import SparkSession
import boto3

# S3 resource
def getDataFromS3():
    s3 = boto3.resource('s3', verify=False)

    s3_client = boto3.client('s3', verify=False)
    bucket_list = s3_client.list_buckets()
    bucket_name = bucket_list['Buckets'][0]['Name']

    files = list(s3.Bucket(bucket_name).objects.all())
    file_key = files[1].key
    file_name = file_key.split('/')[1]

    # s3_client.download_file(bucket_name, file_key, "s3/"+file_name)

    return file_name

def createSparkDataFrame():
    spark = SparkSession.builder.appName('test').getOrCreate()
    file_name = getDataFromS3()
    file_ext = file_name.split('.')[1]
    if file_ext == 'csv':
        df =  spark.read.csv(file_name, header=True, inferSchema=True)
        return df
    elif file_ext == 'parquet':
        df = spark.read.parquet(file_name)
        return df
    elif file_ext == 'json':
        df = spark.read.json(file_name)
        return df
    elif file_ext == 'text':
        df = spark.read.text(file_name)
        return df
    elif file_ext == 'jdbc':
        df = spark.read.jdbc(file_name)
        return df

df = createSparkDataFrame()
df.show(100)