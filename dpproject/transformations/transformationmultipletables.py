from pyspark import pipelines as dp
from pyspark.sql.functions import lit, input_file_name, current_timestamp, col
from utilities.config import sourceName, sourcePath, fileFormat, bronzetabelSchema, fileName

class MyProject:
    def __init__(self):
        self.sourceName = sourceName
        self.sourcePath = sourcePath
        self.fileFormat = fileFormat
        self.bronzetabelSchema = bronzetabelSchema
        self.fileName = fileName

    def getData(fileName: str):
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
                    .option('cloudFiles.format',self.fileFormat)
                    .load(f'{self.sourcePath}/{self.sourceName}/{self.fileName}_*.{self.fileFormat}')
                    .withColumn('fileName',col('_metadata.file_name') )
                    .withColumn('loaddt', current_timestamp())
                )

for table in fileName:
    # print(table)
    @dp.table(
        name = f'{self.bronzetabelSchema}.{table}'  ,
        comment = f"{self.bronzetabelSchema}.{table} Table Created for Source {self.sourceName}/{self.table}"     
    )
    def createTable(fileName=table):  
        return getData(fileName)

