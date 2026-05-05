from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# Configuration variables for the pipeline
env = spark.conf.get('env')
configCatalog = spark.conf.get('configCatalog')
configSchema = spark.conf.get('configSchema')
sourceName = spark.conf.get('sourceName')

configSource = 'source'
configSourceTable = 'sourcetables'

# Read source configuration from the config table
def getSourceConfig():
    
    return (spark.read.table(f"{configCatalog}_{env}.{configSchema}.{configSource}")
          .where(col("shortname") == sourceName)
          .first()
          .asDict()
      )
    

def getSourceTableConfig(sourcetableid : str):

    return (
            spark.read.table(f"{configCatalog}_{env}.{configSchema}.{configSourceTable}")
            .where(col("sourceid") == sourcetableid)
    )
 




    

