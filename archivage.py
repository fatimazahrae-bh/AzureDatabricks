# Databricks notebook source
 # get the destinations of the directories in our container (Raw, Processed, Archivage)
def get_file_path(storage_account_name,storage_account_access_key,container_name):

    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key)

    raw = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw"
    processed = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/processed"
    archivage = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/archivage"
    
    raw_files = dbutils.fs.ls(raw)
    processed_files = dbutils.fs.ls(processed)
    archived_files = dbutils.fs.ls(archivage)

    return [raw_files, processed_files, archived_files]

# COMMAND ----------


# COMMAND ----------

def get_file_duration(path):
    modification_time_ms = path.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to datetime
    duration = (datetime.now() - modification_time).total_seconds() / 60
    return duration

# COMMAND ----------

def archived_raw_files(raw_paths):
    for path in raw_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 3:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/archivage/{path.name}"
            dbutils.fs.mv(source_directory, destination_directory,recurse = True)

# COMMAND ----------

def delete_archived_files(archived_paths):
    for path in archived_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 6:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/archivage/{path.name}"
            dbutils.fs.rm(destination_directory,recurse = True)

# COMMAND ----------

from datetime import datetime

storage_account_name = "fatimazahraestorage"
storage_account_access_key = "hMInODuYZ2hQTHY6mDg5eBMxf9EUFLpWhXtwtHwq88f/te9CBRmr5eeNhyLad6JKmOPGFuv/RqrB+ASt2FlwpQ=="
container_name = "fatimazahraecontainer"

# get files path
files_paths = get_file_path(storage_account_name,storage_account_access_key,container_name)

archived_raw_files(files_paths[0])
delete_archived_files(files_paths[2])
#------------

# COMMAND ----------


