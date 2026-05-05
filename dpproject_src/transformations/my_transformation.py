from pyspark import pipelines as dp
from pyspark.sql.functions import lit, input_file_name, current_timestamp, col
from utilities.config import env,configCatalog, configSchema, sourceName, getSourceConfig, getSourceTableConfig

        
    

def getData(fileName: str, fileFormat:str, sourcePath : str, sourceName:str):
    """ 
        
        This function is used to read the data from the source path and return the dataframe.
        
        Parameters:
            fileFormat (str): The file format of the data.
            sourcePath (str): The path of the source.
            sourceName (str): The name of the source.
            fileName (str): The name of the file.
            
        Returns:
            dataframe: The dataframe of the data.
    """
        # print(f'{sourcePath}/{sourceName}/{fileName}_*.{fileFormat}')
        
        

    return (spark.readStream
                    .format('cloudFiles')
                    .option('cloudFiles.format',fileFormat)
                    .load(f'{sourcePath}/{sourceName}/{fileName}_*.{fileFormat}')
                    .withColumn('fileName',col('_metadata.file_name') )
                    .withColumn('loaddt', current_timestamp())
                )

if __name__ == "__main__":
    print("This is a module to read the data from the source.")

    sconfig = getSourceConfig()
    for table in getSourceTableConfig(sconfig['id']).collect():
        print(table.tablename)
        @dp.table(
            name = f'{table.tablename}'  ,
            comment = f"{table.tablename} Table Created for Source {sourceName}/{table.tablename}"     
        )
        def create_table(t=table):
            return getData( t.filename, t.format, t.filepath,sourceName)

