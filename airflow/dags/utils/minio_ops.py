import io
import re
from minio import Minio

def _get_client(config): 
    return Minio(
        config['host'],
        access_key=config['user'],
        secret_key=config['password'],
        secure=False
    )

def upload_parquet_to_minio(df, object_name, minio_config, bucket_name):
    client = _get_client(minio_config)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    size = parquet_buffer.tell()
    parquet_buffer.seek(0)
    client.put_object(
        bucket_name,
        object_name,
        data=parquet_buffer,
        length=size,
        content_type='application/octet-stream'
    )
    print(f"Upload ok dans {bucket_name}/{object_name}")

def download_files_by_pattern(pattern, minio_config, bucket_name):
    client = _get_client(minio_config)
    objects = client.list_objects(bucket_name, recursive=True)
    
    files = []
    for obj in objects:
        if re.search(pattern, obj.object_name):
            response = client.get_object(bucket_name, obj.object_name)
            try:
                data = io.BytesIO(response.read())
                files.append((obj.object_name, data))
            finally:
                response.close()
                response.release_conn()
    return files 