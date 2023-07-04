from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').getOrCreate()

df = spark.read.parquet(r'C:\Users\Sachin.Pal\Desktop\pyspark_tut\p_files\part-00000-a441f61e-03b8-482c-8f6e-5cc9bd5a3faa-c000.snappy.parquet')
df.show()