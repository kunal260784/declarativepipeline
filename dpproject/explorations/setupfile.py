# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql.functions import col
df = (spark.read.table("myproject_dev.etlconfig.source")
          .where(col("shortname") == 'awk')
          .first()
          .asDict()
      )
print(df['id'])

# COMMAND ----------

# from pyspark import pipelines as dp
from pyspark.sql.functions import lit, input_file_name, current_timestamp, col
# from utilities.config import env,configCatalog, configSchema, sourceName, getSourceConfig, getSourceTableConfig

env = 'dev'
configCatalog = 'myproject'
configSchema = 'etlconfig'
sourceName = 'awk'

configSource = 'source'
configSourceTable = 'sourcetables'


class MyProject:
    def __init__(self, configCatalog, configSchema, sourceName):
        
        self.env = env
        self.configCatalog = configCatalog
        self.configSchema = configSchema
        self.sourceName = sourceName
        self.config = self.getSourceConfig()
        self.tableConfig = self.getSourceTableConfig(self.config['id'])
        
    def getSourceConfig(self):
    
        return (spark.read.table(f"{configCatalog}_{self.env}.{configSchema}.{configSource}")
            .where(col("shortname") == sourceName)
            .first()
            .asDict()
        )
    

    def getSourceTableConfig(self,sourcetableid : str):

        return (
                spark.read.table(f"{configCatalog}_{self.env}.{configSchema}.{configSourceTable}")
                .where(col("sourceid") == sourcetableid)
        )
 

    def getData(self,fileName: str, fileFormat:str, sourcePath : str, sourceName:str):
        """ 
        
        This function is used to read the data from the source path and return the dataframe.
        
        Parameters:
            fileFormat (str): The file format of the data.
            sourcePath (str): The path of the source.
            sourceName (str): The name of the source.
            fileName (str): The name of the file.
            
        Returns:
            dataframe: The dataframe of the data."""
        # print(f'{sourcePath}/{sourceName}/{fileName}_*.{fileFormat}')
        
        

        return (spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format',fileFormat)
                    .load(f'{sourcePath}/{sourceName}/{fileName}_*.{fileFormat}')
                    .withColumn('fileName',col('_metadata.file_name') )
                    .withColumn('loaddt', current_timestamp())
                )


    def processTable(self):

        for table in self.tableConfig.collect():
            # print(table)
            print(
                f'{self.configCatalog}_{self.env}.{table.schema}_{self.sourceName}.{table.tablename}'  ,
                f"{table.schema}_{self.sourceName}.{table.tablename} Table Created for Source {self.sourceName}/{table.tablename}"     
            )
            def createTable(table):  
                print(table.filename, table.format, table.filepath, table.sourceName)


if __name__ == "__main__":
    myProject = MyProject(configCatalog, configSchema, sourceName)
    myProject.processTable()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM myproject_dev.etlconfig.sourcetables

# COMMAND ----------

df = (spark.read.table('myproject_dev.etlconfig.sourcetables')
            .where(col("sourceid") == '8d7746dc-af75-4468-ac03-b582d6bee5b4')
            .first()
            .asDict()
)
