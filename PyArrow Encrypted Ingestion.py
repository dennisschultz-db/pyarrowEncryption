# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Azure Data Lake Storage (ADLS) and Databricks
# MAGIC
# MAGIC <img src="https://oneenvstorage.blob.core.windows.net/images/adlslogo.png" width="400">
# MAGIC
# MAGIC | Specs                |                                   |
# MAGIC |----------------------|-----------------------------------|
# MAGIC | Azure Resource Group | dennis_schultz_rg                 |
# MAGIC | ADLS Account         | dennisschultzstorage                        |
# MAGIC | Container           | landing_zone                  |
# MAGIC | Region               | US East 2                           | 
# MAGIC
# MAGIC <br />
# MAGIC
# MAGIC
# MAGIC Docs: https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html

# COMMAND ----------

# DBTITLE 1,Install updated versions of pyarrow and pytest
# MAGIC %pip install pyarrow==13.0.0
# MAGIC %pip install pytest==7.4.2

# COMMAND ----------

# DBTITLE 1,Imports
import tarfile
from datetime import timedelta

import pytest
import pyarrow as pa
try:
    import pyarrow.parquet as pq
    import pyarrow.parquet.encryption as pe
except ImportError:
    pq = None
    pe = None
else:
    from pyarrow.tests.parquet.encryption import (
        InMemoryKmsClient, verify_file_encrypted)


# COMMAND ----------

# DBTITLE 1,Static variable values
MOUNT_POINT = '/mnt/landing-zone'

TARFILE_NAME = "/dbfs" + MOUNT_POINT + "/transport.tgz"

# COMMAND ----------

# MAGIC %md
# MAGIC # Mount ADLS storage container
# MAGIC The cell below should only need to be run once to mount the external container as a mount point on DBFS

# COMMAND ----------

# The below details are related to the Service Principal oneenv-adls
APPLICATION_ID = "ed573937-9c53-4ed6-b016-929e765443eb"
DIRECTORY_ID = "9f37a392-f0ae-4280-9796-f1864a10effc"
# You can use the below if you don't want to use key vault
APP_KEY = "rNH8Q~q8IMYIsjntqqnJcN7yxCqRXfbsOj8_ba3b"
#APP_KEY = dbutils.secrets.get(scope = "oneenvkeys", key = "adls-app-key")

STORAGE_ACCOUNT = "oneenvadls"
CONTAINER = "dennis-schultz"
PATH = ""

# Unmount existing mount if it exists
if MOUNT_POINT in [s.mountPoint for s in dbutils.fs.mounts()]:
  dbutils.fs.unmount(MOUNT_POINT)

# Configuration settings for mount
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": APPLICATION_ID,
          "fs.azure.account.oauth2.client.secret": APP_KEY,
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + DIRECTORY_ID + "/oauth2/token"}

dbutils.fs.mount(
  source= "abfss://"+CONTAINER+"@"+STORAGE_ACCOUNT+".dfs.core.windows.net/"+PATH,
  mount_point= MOUNT_POINT,
  extra_configs=configs
)

# COMMAND ----------

# DBTITLE 1,Confirm tar file has been uploaded to mount point

display(dbutils.fs.ls(MOUNT_POINT))

# COMMAND ----------

# DBTITLE 1,Extract files from tar file
tar = tarfile.open(TARFILE_NAME, mode='r')
tar.extractall('/dbfs' + MOUNT_POINT + "/extracted")
tar.close()


# COMMAND ----------

# DBTITLE 1,Extracted files are still encrypted
list = dbutils.fs.ls(MOUNT_POINT + "/extracted/temp")
display(list)
for file in list:
  display(file.name + ":  " + dbutils.fs.head(MOUNT_POINT + "/extracted/temp/" +file.name, 256) )

# COMMAND ----------

# DBTITLE 1,Decrypt files
def read_encrypted_parquet(path, decryption_config, kms_connection_config, crypto_factory):

    file_decryption_properties = crypto_factory.file_decryption_properties(
        kms_connection_config, decryption_config)
    assert file_decryption_properties is not None

    result = pq.ParquetFile(
        path, decryption_properties=file_decryption_properties)
    return result.read(use_threads=True)



# ===================================================

PARQUET_NAME = 'encrypted_table.parquet'
TEXT_NAME = 'encrypted_text.txt'
FOOTER_KEY = b"0123456789112345"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"


# Read with decryption properties
decryption_config = pe.DecryptionConfiguration(
    cache_lifetime=timedelta(minutes=5.0))

kms_connection_config = pe.KmsConnectionConfig(
    custom_kms_conf={
        FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
        COL_KEY_NAME: COL_KEY.decode("UTF-8"),
    }
)

def kms_factory(kms_connection_configuration):
    return InMemoryKmsClient(kms_connection_configuration)

crypto_factory = pe.CryptoFactory(kms_factory)

result_table = read_encrypted_parquet(
    "/dbfs" + MOUNT_POINT + "/extracted/temp/" + PARQUET_NAME, 
    decryption_config=decryption_config, 
    kms_connection_config=kms_connection_config, 
    crypto_factory=crypto_factory)

pdf = result_table.to_pandas()
print('-------------------------------------------')
print('   Data read from encrypted parquet file')
print('-------------------------------------------')
print(pdf)

results_txt = read_encrypted_parquet(
    "/dbfs" + MOUNT_POINT + "/extracted/temp/" + TEXT_NAME,
    decryption_config=decryption_config,
    kms_connection_config=kms_connection_config,
    crypto_factory=crypto_factory
).to_pandas()

print('-------------------------------------------')
print('   Data read from encrypted parquet file')
print('-------------------------------------------')
text_value = results_txt.loc[0]["payload"]
display(text_value)

# COMMAND ----------

# DBTITLE 1,Save tabular data as Delta Table
spark.sql("CREATE SCHEMA IF NOT EXISTS dennis_schultz")
spark.sql("USE SCHEMA dennis_schultz")
df = spark.createDataFrame(pdf)
df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable('transported_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transported_table

# COMMAND ----------

display(text_value)

# COMMAND ----------


