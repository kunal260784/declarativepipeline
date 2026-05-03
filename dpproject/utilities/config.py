from pyspark.sql.functions import col

# Configuration variables for the pipeline
env = spark.conf.get('env')
configCatalog = spark.conf.get('configCatalog')
configSchema = spark.conf.get('configSchema')
sourceName = spark.conf.get('sourceName')

# Read source configuration from the config table
def getSourceConfig():
    
    return (spark.read.table(f"{configCatalog}.{configSchema}.{sourceName}")
          .where(col("shortname") == sourceName)
          .first()
          .asDict()
      )
 

# Read the source data
def readSourceData():
    df = spark.read.table(f"{env}.{sourceName}")
    return df

# Write the target data
def writeTargetData()
    
    

