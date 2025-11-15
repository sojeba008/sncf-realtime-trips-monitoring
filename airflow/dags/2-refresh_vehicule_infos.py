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
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': (os.getenv('DB_PORT'))
}

def decoder_numero_train(num_train: str) -> dict:
    """
    Decode possible information from a train number (it shoudl consists of digits only).
    """
    train_info = {
        "num_train": num_train,
        "sens_circulation": None,
        "type_train": None,
        "sous_type": None,
        "categorie_service": None,
        "axe_geographique": None,
        "region_origine": None,
        "vitesse_max_kmh": None,
        "plage_numero": None,
        "code_mission": None,
        "origine_mission": None,
        "destination_mission": None,
        "periode_service": None,
        "commentaire": "",
        "date_creation": datetime.now(),
        "date_maj": datetime.now(),
    }

    if not num_train.isdigit():
        train_info["commentaire"] = "Numéro non numérique – non décodable"
        return train_info

    numero = int(num_train)
    train_info["sens_circulation"] = "Pair" if numero % 2 == 0 else "Impair"

    if 1 <= numero <= 499:
        train_info.update({
            "type_train": "Classique GL International",
            "categorie_service": "Voyageurs",
            "axe_geographique": "International",
            "plage_numero": "1–499"
        })
    elif 7000 <= numero <= 7999:
        train_info.update({
            "type_train": "TGV",
            "categorie_service": "Voyageurs",
            "axe_geographique": "Nord Est",
            "plage_numero": "7000–7999"
        })
    elif 8000 <= numero <= 8999:
        train_info.update({
            "type_train": "TGV",
            "categorie_service": "Voyageurs",
            "axe_geographique": "Atlantique",
            "plage_numero": "8000–8999"
        })
    elif 50000 <= numero <= 50199:
        train_info.update({
            "type_train": "Messagerie Express",
            "categorie_service": "Fret",
            "vitesse_max_kmh": 160,
            "plage_numero": "50000–50199"
        })
    elif 52000 <= numero <= 52999:
        train_info.update({
            "type_train": "Combiné",
            "categorie_service": "Fret",
            "vitesse_max_kmh": 100,
            "plage_numero": "52000–52999"
        })
    elif 70000 <= numero <= 99999:
        train_info.update({
            "type_train": "Train Entier",
            "categorie_service": "Fret",
            "plage_numero": "70000–99999"
        })
    elif 300000 <= numero <= 399999:
        train_info.update({
            "type_train": "Machines seules",
            "categorie_service": "Service",
            "plage_numero": "300000–399999"
        })
    elif 700000 <= numero <= 749999:
        train_info.update({
            "type_train": "Train Vide (W)",
            "categorie_service": "Voyageurs vides",
            "plage_numero": "700000–749999"
        })
    elif 750000 <= numero <= 799999:
        train_info.update({
            "type_train": "Retour Vide / Acheminement",
            "categorie_service": "Voyageurs vides",
            "plage_numero": "750000–799999"
        })
    elif 854000 <= numero <= 856999:
        train_info.update({
            "type_train": "TER",
            "region_origine": "Bretagne",
            "categorie_service": "Voyageurs",
            "plage_numero": "854000–856999"
        })
    elif 110000 <= numero <= 119999:
        train_info.update({
            "type_train": "Transilien",
            "region_origine": "Paris Est / RER E",
            "categorie_service": "Voyageurs",
            "plage_numero": "110000–119999"
        })
    else:
        train_info["commentaire"] = "Numéro inconnu ou hors plage"

    return train_info


def refresh_vehicule_infos():
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT train, split_part(vehicule_category, '::', 2) AS category_vehicule, vehicule_mode FROM dsa.trips")
            rows = cur.fetchall()

            train_data = []
            for row in rows:
                decoded = decoder_numero_train(row[0])
                if decoded:
                    values = (
                        decoded['num_train'],
                        row[1],
                        row[2],
                        decoded['sens_circulation'],
                        decoded['type_train'],
                        decoded['sous_type'],
                        decoded['categorie_service'],
                        decoded['axe_geographique'],
                        decoded['region_origine'],
                        decoded['vitesse_max_kmh'],
                        decoded['plage_numero'],
                        decoded['code_mission'],
                        decoded['origine_mission'],
                        decoded['destination_mission'],
                        decoded['periode_service'],
                        decoded['commentaire'],
                        decoded['date_creation'],
                        decoded['date_maj']
                    )
                    train_data.append(values)

            if train_data:
                execute_values(cur, """
                    INSERT INTO dwh.d_vehicule (
                        num_vehicule,
                        categorie_vehicule,
                        vehicule_mode,
                        sens_circulation,
                        type_train,
                        sous_type,
                        categorie_service,
                        axe_geographique,
                        region_origine,
                        vitesse_max_kmh,
                        plage_numero,
                        code_mission,
                        origine_mission,
                        destination_mission,
                        periode_service,
                        commentaire,
                        date_creation,
                        date_maj
                    ) VALUES %s
                    ON CONFLICT (num_vehicule) DO NOTHING;
                """, train_data)

        conn.commit()

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1),
}

with DAG(
    'refresh_vehicule_infos',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    catchup=False,
    tags=[],
    max_active_runs=1
) as dag:
    
    collect_task = PythonOperator(
        task_id='3-refresh_vehicule_infos',
        python_callable=refresh_vehicule_infos
    )
    

    collect_task