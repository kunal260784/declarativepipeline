# Databricks notebook source
# MAGIC %sql
# MAGIC -- INSERT INTO dpproject_dev.etlconfig.source
# MAGIC -- SELECT uuid(), 'AdventureWorks','src','SQL Server Database Storing Information About Finance Data', current_timestamp(), current_timestamp(), current_user(), current_user();
# MAGIC
# MAGIC INSERT INTO dpproject_dev.etlconfig.sourcetables
# MAGIC   SELECT uuid(), '18fe14ca-2b97-485d-9209-e5dac7abffe2', 'finance', 'Table Includes Financial Transactions', 'dpproject', 'bronze', 'AdventureWorks_Finance', '/Volumes/dpproject_dev/landing','parquet','N',NULL,'N',NULL, current_timestamp(), current_timestamp(), current_user(), current_user()
# MAGIC UNION ALL
# MAGIC   SELECT uuid(), '18fe14ca-2b97-485d-9209-e5dac7abffe2', 'organization', 'Table Includes Organization Details. It''s a reference data', 'dpproject', 'bronze', 'AdventureWorks_Organization', '/Volumes/dpproject_dev/landing','parquet','N',NULL,'N',NULL,  current_timestamp(), current_timestamp(), current_user(), current_user()
# MAGIC UNION ALL
# MAGIC   SELECT uuid(), '18fe14ca-2b97-485d-9209-e5dac7abffe2', 'scenario', 'Table Includes Scenario Details', 'dpproject', 'bronze', 'AdventureWorks_Scenario', '/Volumes/dpproject_dev/landing', 'parquet','N',NULL,'N',NULL, current_timestamp(), current_timestamp(), current_user(), current_user()
# MAGIC UNION ALL
# MAGIC   SELECT uuid(), '18fe14ca-2b97-485d-9209-e5dac7abffe2', 'account', 'Table Includes Account Details', 'dpproject', 'bronze', 'AdventureWorks_account', '/Volumes/dpproject_dev/landing','parquet','N',NULL,'N',NULL,  current_timestamp(), current_timestamp(), current_user(), current_user()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dpproject_dev.etlconfig.sourcetables
