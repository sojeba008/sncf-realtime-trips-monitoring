import pandas as pd
import s3fs
fs = s3fs.S3FileSystem(
        key='minioadmin',
        secret='minioadmin',
        endpoint_url="http://127.0.0.1:9000",
        use_ssl=False
    )
    
path = "sncf-trips-datasets/NB_TRAINS/"
df = pd.read_parquet(f"{path}", filesystem=fs)