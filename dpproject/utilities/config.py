from pyspark.sql import SparkSession

# Initialize the spark session
spark = SparkSession.builder \
    .appName("MyDeclarativePipeline") \
    .getOrCreate()

env = spark.config.get('env')

