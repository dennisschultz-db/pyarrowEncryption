from datetime import timedelta

import tarfile
import os.path
import shutil
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


def write_encrypted_parquet(path, table, encryption_config, kms_connection_config, crypto_factory):
    file_encryption_properties = crypto_factory.file_encryption_properties(
        kms_connection_config, encryption_config)
    assert file_encryption_properties is not None
    with pq.ParquetWriter(
            path, table.schema,
            encryption_properties=file_encryption_properties) as writer:
        writer.write_table(table)


def retrieve_tabular_data():
     # Sample data, representative of data read from client System of Record
    return pa.Table.from_pydict({
        'CustomerID': pa.array([1001, 1002, 1003]),
        'FirstName': pa.array(['Jane', 'Bob', 'Sam']),
        'LastName': pa.array(['Jones', 'Baker', 'Sloan']),
        'SSN': pa.array(['123-45-6789', '234-56-7890', '345-67-8901'])
    })

def retrieve_text_data():
    return "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."

def store_text_as_table(data):
    return pa.Table.from_pydict({
        'payload': pa.array([data])
    })

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def upload_to_landing_zone(storage_url, container_name, blob_name, content_path):
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient

    # For demonstration only.  Will use IDE or CLI credentials
    credential = DefaultAzureCredential()

    blob_client = BlobClient(
        storage_url,
        container_name=container_name,
        blob_name=blob_name,
        credential=credential,
    )

    with open(content_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f'Uploaded {data.name} to {blob_client.url}')





# ===================================================

TEMPDIR = './temp'
PARQUET_NAME = 'encrypted_table.parquet'
TEXT_NAME = 'encrypted_text.txt'
BLOB_NAME = 'transport.tgz'
FOOTER_KEY = b"0123456789112345"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"

path = TEMPDIR + '/' + PARQUET_NAME
text_path = TEMPDIR + '/' + TEXT_NAME

# Clean up any past runs
shutil.rmtree(TEMPDIR)
os.mkdir(TEMPDIR)

# Simulates connector to client database or system
table_data = retrieve_tabular_data()
# Simulates connector to an unstructured data source
text_data = retrieve_text_data()




# Encrypt the footer with the footer key,
# encrypt columns `FirstName`, `LastName`, and `SSN` with the column key,
# keep `CustomerID` plaintext
encryption_config = pe.EncryptionConfiguration(
    footer_key=FOOTER_KEY_NAME,
    column_keys={
        COL_KEY_NAME: ['FirstName', 'LastName', 'SSN'],
    },
    encryption_algorithm="AES_GCM_V1",
    cache_lifetime=timedelta(minutes=5.0),
    data_key_length_bits=256)

kms_connection_config = pe.KmsConnectionConfig(
    custom_kms_conf={
        FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
        COL_KEY_NAME: COL_KEY.decode("UTF-8"),
    }
)

def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

crypto_factory = pe.CryptoFactory(kms_factory)



# Write data to local file at path with encryption properties
write_encrypted_parquet(
    path, 
    table=table_data, 
    encryption_config=encryption_config,
    kms_connection_config=kms_connection_config, 
    crypto_factory=crypto_factory)


# Embed unstructured text as column of a table
text_table_data = store_text_as_table(text_data)

# Write embedded text table with encryption properties
encryption_config.column_keys={COL_KEY_NAME: ['payload']}
write_encrypted_parquet(
    text_path,
    table=text_table_data,
    encryption_config=encryption_config,
    kms_connection_config=kms_connection_config,
    crypto_factory=crypto_factory)


# Tar/zip all the encrypted files into one archive for transport
tarfile_name = TEMPDIR + '/transport.tgz'
make_tarfile(
    output_filename=tarfile_name,
    source_dir=TEMPDIR
)

# Upload the encrypted file to Azure container landing zone
# The archive could also be physically transported to the landing zone, if required by client
storage_url = "https://oneenvadls.blob.core.windows.net"
container_name = "dennis-schultz"

upload_to_landing_zone(
    storage_url=storage_url,
    container_name=container_name,
    blob_name=BLOB_NAME,
    content_path=tarfile_name
)
