import io
import re
from minio import Minio
import pandas as pd
import s3fs
from datetime import datetime
import json
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import os
from io import BytesIO

def _get_minio_client(config): 
    return Minio(
        config['host'],
        access_key=config['user'],
        secret_key=config['password'],
        secure=False
    )

def upload_parquet_to_minio(df, object_name, minio_config, bucket_name):
    client = _get_minio_client(minio_config)
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
    client = _get_minio_client(minio_config)
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

def load_nb_trains_dataset_from_s3(bucket: str, folder: str, endpoint_url: str, access_key: str, secret_key: str, use_listings_cache: bool, use_ssl: bool ) -> pd.DataFrame:
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        endpoint_url=endpoint_url,
        use_ssl=use_ssl,
        use_listings_cache=use_listings_cache
    )
    s3_path = f"{bucket}/{folder}"
    df = pd.read_parquet(
        s3_path,
        filesystem=fs
    )
    return df


def nb_trains_model_from_s3(MINIO_PARAMS, BUCKET_NAME :str):
    print(MINIO_PARAMS)
    df = load_nb_trains_dataset_from_s3(
        bucket=BUCKET_NAME,
        folder="NB_TRAINS",
        endpoint_url=MINIO_PARAMS['host'],
        access_key=MINIO_PARAMS['user'],
        secret_key=MINIO_PARAMS['password'],
        use_listings_cache=MINIO_PARAMS['use_listings_cache'],
        use_ssl=MINIO_PARAMS['use_ssl']
    )
    df = df.sort_values("hour_start").reset_index(drop=True)
    df["hour"] = df["hour_start"].dt.hour
#    df["nb_trains_actifs"] = df.groupby("hour")["nb_trains_actifs"].transform(
#        lambda x: x.fillna(x.mean())
#    )

    feature_cols = [
        "hour",
        "is_weekend",
        "day_in_month",
        "is_first_day_of_month",
        "is_first_day_of_week",
        "is_first_month_of_quarter",
        "is_last_day_of_month",
        "is_last_day_of_week",
        "is_last_month_of_quarter",
        "day_in_the_week",
        "is_public_holiday"
    ]

    #X = df[feature_cols]
    y = df["nb_trains_actifs"]

    split_idx = int(len(df) * 0.8)
    X_train, X_test = df.iloc[:split_idx], df.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    last_date_x_train = X_train["hour_start"].max()
    X_train = X_train[feature_cols]
    X_test = X_test[feature_cols]

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    last_date = df["hour_start"].max()

    #model_dir = Path(model_dir)
    #model_dir.mkdir(parents=True, exist_ok=True)

    model_date = last_date.strftime("%Y-%m-%d")
    #model_path = model_dir / f"nb_trains_model_{model_date}.joblib"
    #meta_path = model_dir / f"nb_trains_model_{model_date}.json"

    #joblib.dump(model, model_path)
### 
    fs = s3fs.S3FileSystem(
        key=MINIO_PARAMS['user'],
        secret=MINIO_PARAMS['password'],
        endpoint_url=MINIO_PARAMS['host'],
        use_ssl=MINIO_PARAMS['use_listings_cache']
    )
    model_s3_path = f"{BUCKET_NAME}/ML_MODELS/nb_trains/nb_trains_model_{model_date}.joblib"

    buffer = BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)

    with fs.open(model_s3_path, "wb") as f:
        f.write(buffer.read())
### 
    metadata = {
        "model_name": f"nb_trains_model_{model_date}",
        "trained_until": str(last_date_x_train),
        "mse": mse,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "n_rows": len(df),
        "features": feature_cols,
        "created_at": datetime.utcnow().isoformat()
    }

    #with open(meta_path, "w") as f:
    #    json.dump(metadata, f, indent=4)
    #NEW 
    meta_s3_path = f"{BUCKET_NAME}/ML_MODELS/nb_trains/nb_trains_model_{model_date}.json"
    with fs.open(meta_s3_path, 'w') as f:
        json.dump(metadata, f, indent=4)
    #NEW FIN
    return metadata

def load_latest_model_from_s3(bucket: str, folder: str, fs):
    path_pattern = f"{bucket}/{folder}/*.joblib"
    files = fs.glob(path_pattern)
    if not files:
        raise FileNotFoundError(f"No result for: {path_pattern}")
    latest_file = max(files, key=lambda f: fs.info(f)['LastModified'])
    meta_file = latest_file.replace(".joblib", ".json")
    return latest_file, meta_file