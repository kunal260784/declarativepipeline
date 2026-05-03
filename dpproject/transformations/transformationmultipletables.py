from pyspark import pipelines as dp
from pyspark.sql.functions import lit, input_file_name, current_timestamp, col
from utilities.config import sourceName, sourcePath, fileFormat, bronzetabelSchema, fileName

def getData(file_name: str):
    """ 
    
    This function is used to read the data from the source path and return the dataframe.
    
    Parameters:
        fileFormat (str): The file format of the data.
        sourcePath (str): The path of the source.
        sourceName (str): The name of the source.
        file_name (str): The name of the file.
        
    Returns:
        dataframe: The dataframe of the data."""
    # print(f'{sourcePath}/{sourceName}/{file_name}_*.{fileFormat}')
    return (spark.readStream
                .format('cloudFiles')
                .option('cloudFiles.format', fileFormat)
                .load(f'{sourcePath}/{sourceName}/{file_name}_*.{fileFormat}')
                .withColumn('fileName', col('_metadata.file_name'))
                .withColumn('loaddt', current_timestamp())
            )

for table in fileName:
    # print(table)
    @dp.table(
        name=f'{bronzetabelSchema}.{table}',
        comment=f"{bronzetabelSchema}.{table} Table Created for Source {sourceName}/{table}"     
    )
    def createTable(file_name=table):  
        return getData(file_name)

