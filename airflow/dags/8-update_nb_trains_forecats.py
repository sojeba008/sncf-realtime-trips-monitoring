from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from pathlib import Path
from utils.minio_ops import load_latest_model_from_s3
import s3fs
import json
import joblib
from psycopg2.extras import execute_values

def update_nb_trains_forecats():
    load_dotenv()
    fs = s3fs.S3FileSystem(
    key=os.getenv("MINIO_ROOT_USER"),
    secret=os.getenv("MINIO_ROOT_PASSWORD"), 
    endpoint_url="http://"+os.getenv("MINIO_HOST"),
    use_ssl=os.getenv("S3_USE_SSL").lower() == 'true',
    use_listings_cache= os.getenv("S3_USE_LISTINGS_CACHE").lower() == 'true'
    )
    DB_PARAMS = {
        'host': os.getenv('DWH_HOST'),
        'dbname': os.getenv('DWH_DB'),
        'user': os.getenv('DWH_USER'),
        'password': os.getenv('DWH_PASSWORD'),
        'port': os.getenv('DWH_PORT')
    }
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MODEL_FOLDER = "ML_MODELS/nb_trains"

    latest_model_path, latest_meta_path = load_latest_model_from_s3(BUCKET_NAME, MODEL_FOLDER, fs)

    print(f"load of : {latest_model_path}")

    #JSON metadata
    with fs.open(latest_meta_path, 'r') as f:
        metadata = json.load(f)

    # joblib
    with fs.open(latest_model_path, 'rb') as f:
        model = joblib.load(f)

    last_known_date = pd.Timestamp(metadata["trained_until"])
    start_datetime = pd.Timestamp(last_known_date, tz=None)

    start_datetime_str = str(start_datetime)
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    sql = """
        WITH times_list AS (
            SELECT
                hour_delta AS hour_start,
                TO_CHAR(hour_delta, 'HH24')::INT4 AS hour_delta
            FROM generate_series(
                    %s::timestamp+INTERVAL '1 hour',
                    now()::timestamp,
                    INTERVAL '1 hour'
                ) AS gs(hour_delta)
        )
        SELECT
            tl.hour_start,
            tl.hour_delta AS hour,
            dd.day_in_month,
            to_char(dd.date, 'D')::INT4 AS day_in_the_week,
            CASE 
                WHEN to_char(dd.date, 'D')::INT4 IN (1,7) THEN TRUE
                ELSE FALSE
            END AS is_weekend, 
            dd.is_first_day_of_month,
            dd.is_last_day_of_month,
            dd.is_first_day_of_week,
            dd.is_last_day_of_week,
            dd.is_first_month_of_quarter,
            dd.is_last_month_of_quarter,
            COALESCE(dd.is_public_holiday, FALSE) AS is_public_holiday
        FROM dwh.d_date dd
        INNER JOIN times_list tl ON tl.hour_start::DATE = dd.date
        ORDER BY tl.hour_start;
        """
    future_df = pd.read_sql(sql, conn, params=[start_datetime_str])


    print(future_df.head(5))

    feature_cols = metadata["features"]
    X_future = future_df[feature_cols]

    future_df["predicted_nb_trains_actifs"] = model.predict(X_future)
    print(future_df.head())
    print(future_df["predicted_nb_trains_actifs"])
    cur.execute("TRUNCATE TABLE forecast.fact_train_volume_forecast RESTART IDENTITY;")

    records_to_insert = [
        (row["hour_start"], int(row["predicted_nb_trains_actifs"]), metadata["model_name"])
        for _, row in future_df.iterrows()
    ]

    insert_query = """
    INSERT INTO forecast.fact_train_volume_forecast (date, predicted_nb_train, model_name)
    VALUES %s
    """
    execute_values(cur, insert_query, records_to_insert)
    conn.commit()
    cur.close()
    conn.close()


default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'update_nb_trains_forecats',
    default_args=default_args,
    schedule_interval='0 2 * * *', 
    catchup=False,
    tags=['ML', 'Daily'],
    max_active_runs=1
) as dag:
    
    update_nb_trains_forecats_task = PythonOperator(
        task_id='update_nb_trains_forecats',
        python_callable=update_nb_trains_forecats
    )