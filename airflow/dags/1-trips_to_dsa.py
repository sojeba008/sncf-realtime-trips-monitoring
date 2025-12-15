import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_values

load_dotenv()
DB_PARAMS = {
    'host': os.getenv('DWH_HOST'),
    'dbname': os.getenv('DWH_DB'),
    'user': os.getenv('DWH_USER'),
    'password': os.getenv('DWH_PASSWORD'),
    'port': (os.getenv('DWH_PORT'))
}

URL = "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-estimated-timetable"

def trip_to_dsa():
    response = requests.get(URL)
    if response.status_code != 200:
        raise Exception(f"Erreur: {response.status_code}")

    root = ET.fromstring(response.content)
    ns = {'siri': 'http://www.siri.org.uk/siri'}
    journeys = root.findall('.//siri:EstimatedVehicleJourney', ns)

    production_date = datetime.now()
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dsa.trips CASCADE;")
    cursor.execute("TRUNCATE TABLE dsa.stops CASCADE;")

    trip_rows = []
    stop_rows = []
    i = 0
    for journey in journeys:
        i += 1
        print("Journey : " + str(i))
        vj_ref = journey.findtext('.//siri:VehicleJourneyRef', default="N/A", namespaces=ns)
        date = journey.findtext('.//siri:DataFrameRef', default="N/A", namespaces=ns)
        dated_vj_ref = journey.findtext('.//siri:DatedVehicleJourneyRef', default="N/A", namespaces=ns)
        
        unique_id = f"{dated_vj_ref}"

        train_num = journey.findtext('.//siri:TrainNumberRef', default="N/A", namespaces=ns)
        origin_name = journey.findtext('.//siri:OriginName', default="N/A", namespaces=ns)
        departure_time = journey.findtext('.//siri:OriginAimedDepartureTime', default="N/A", namespaces=ns)
        dest_name = journey.findtext('.//siri:DestinationName', default="N/A", namespaces=ns)
        arrival_time = journey.findtext('.//siri:DestinationAimedArrivalTime', default="N/A", namespaces=ns)

        vehicule_category = journey.findtext('.//siri:ProductCategoryRef', default="N/A", namespaces=ns)
        vehicule_mode = journey.findtext('.//siri:VehicleMode', default="N/A", namespaces=ns)
        published_line_name = journey.findtext('.//siri:PublishedLineName', default="N/A", namespaces=ns)

        trip_rows.append((unique_id, train_num, origin_name, departure_time, dest_name, arrival_time, vehicule_category, vehicule_mode, published_line_name,  production_date))

        calls = journey.findall('.//siri:RecordedCall', ns)
        for idx, call in enumerate(calls):
            stop_name = call.findtext('siri:StopPointName', default="", namespaces=ns)
            aimed_arrival = call.findtext('siri:AimedArrivalTime', default="", namespaces=ns)
            expected_arrival = call.findtext('siri:ExpectedArrivalTime', default="", namespaces=ns)
            aimed_departure = call.findtext('siri:AimedDepartureTime', default="", namespaces=ns)
            expected_departure = call.findtext('siri:ExpectedDepartureTime', default="", namespaces=ns)

            departure_platform_name = call.findtext('siri:DeparturePlatformName', default="", namespaces=ns)
            arrival_platform_name = call.findtext('siri:ArrivalPlatformName', default="", namespaces=ns)

            stop_rows.append((
                unique_id, stop_name, aimed_arrival, expected_arrival,
                aimed_departure, expected_departure,
                1 if idx == 0 else 0,
                1 if stop_name == dest_name else 0,
                departure_platform_name,
                arrival_platform_name,
                production_date
            ))

    execute_values(cursor, """
        INSERT INTO dsa.trips (trip_id, train, origin_name, departure_time, dest_name, arrival_time, vehicule_category, vehicule_mode, published_line_name, production_date)
        VALUES %s
        ON CONFLICT (trip_id) DO NOTHING;
    """, trip_rows)

    execute_values(cursor, """
        INSERT INTO dsa.stops (trip_id, stop_name, aimed_arrival, expected_arrival, aimed_departure, expected_departure, is_starting_point, is_terminus, departure_platform_name, arrival_platform_name, production_date)
        VALUES %s
    """, stop_rows)

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'trip_to_dsa_processing',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=[],
    max_active_runs=1
) as dag:
    
    collect_task = PythonOperator(
        task_id='1-trips_to_dsa',
        python_callable=trip_to_dsa
    )

    trigger_next_run_1 = TriggerDagRunOperator(
        task_id='2-refresh_station_infos',
        trigger_dag_id='refresh_station_infos',
        wait_for_completion=False
    )

    trigger_next_run_2 = TriggerDagRunOperator(
        task_id='3-refresh_ods_table',
        trigger_dag_id='refresh_ods_table',
        wait_for_completion=False
    )
    collect_task >> trigger_next_run_1 >> trigger_next_run_2