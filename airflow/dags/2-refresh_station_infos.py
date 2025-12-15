from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os
from datetime import datetime, date, timedelta
import requests
# from google.transit import gtfs_realtime_pb2
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from psycopg2.extras import execute_values

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DWH_HOST'),
    'dbname': os.getenv('DWH_DB'),
    'user': os.getenv('DWH_USER'),
    'password': os.getenv('DWH_PASSWORD'),
    'port': (os.getenv('DWH_PORT'))
}


def build_station_rows(records):
    rows = []
    for i in range(len(records)):
        r = records.iloc[i]
        rows.append((
            r["code_uic"],
            r["libelle"],
            r["fret"],
            r["voyageurs"],
            r["code_ligne"],
            int(r["rg_troncon"]),
            r["pk"],
            r["commune"],
            r["departemen"],
            int(r["idreseau"]),
            r["idgaia"],
            r["x_l93"],
            r["y_l93"],
            r["x_wgs84"],
            r["y_wgs84"],
            f"SRID=4326;POINT({r['x_wgs84']} {r['y_wgs84']})",
            f"SRID=2154;POINT({r['x_l93']} {r['y_l93']})"
        ))
    return rows


def refresh_station_infos():
    dag_folder = os.path.dirname(__file__)
    ressource_path = os.path.join(dag_folder, 'ressources', 'liste-des-gares.parquet')
    df = pd.read_parquet(ressource_path)
    station_rows =  build_station_rows(df)
    df = df.astype(object)
    print(df)
    
    print(len(station_rows))
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ods.stations RESTART IDENTITY;")

            execute_values(cur, """
                INSERT INTO ods.stations (
                    code_uic, libelle, fret, voyageurs, code_ligne, rg_troncon,
                    pk, commune, departemen, idreseau, idgaia,
                    x_l93, y_l93, x_wgs84, y_wgs84,
                    geom, geom_l93
                )
                VALUES %s;
            """, station_rows)
        conn.commit()
        #conn.cursor.close()
    #conn.close()
# fetch_and_push()

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'refresh_station_infos',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=[],
    max_active_runs=1
) as dag:
    
    collect_task = PythonOperator(
        task_id='3-refresh_station_infos',
        python_callable=refresh_station_infos
    )
    

    collect_task