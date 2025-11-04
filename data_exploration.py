import pandas as pd

# lecture directe du fichier Parquet dans MinIO
df = pd.read_parquet(
    "s3://raw/hubspot/contacts/2025_10_20_1760978040529_1.parquet",
    storage_options={
        "key": "minio",
        "secret": "minio12345",
        "client_kwargs": {
            "endpoint_url": "http://localhost:9000"
        }
    }
)

print(df.head(10)) 