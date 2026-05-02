from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utilities.config import sourceName, sourcePath, fileFormat, silverTableSchema, fileName, bronzetabelSchema

# def createSilverTable(fileName: str):

#     """ 
    
    
#     """

#     dp.create_streaming_table(
#             name=f"{silverTableSchema}.{tbl}",
#             comment = f"Table {tbl} created from {sourceName} source"
#         )
    
# def loadSilverTable(fileName: str):

#     """ 
    
    
#     """

#     dp.create_auto_cdc_flow(
#                     target = f"{silvertabelSchema}.{tbl}",
#                     source = "<data-source>",
#                     keys = ["key1", "key2", "keyN"],
#                     sequence_by = "<sequence-column>",
#                     ignore_null_updates = <bool>,
#                     apply_as_deletes = None,
#                     apply_as_truncates = None,
#                     column_list = None,
#                     except_column_list = None,
#                     stored_as_scd_type = <type>,
#                     track_history_column_list = None,
#                     track_history_except_column_list = None,
#                     name = None,
#                     once = <bool>
#                     )



# for tbl in fileName:
#     createSilverTable(tbl)



            