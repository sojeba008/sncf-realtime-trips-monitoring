from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer
import json
import xml.etree.ElementTree as ET
from airflow.utils.trigger_rule import TriggerRule

KAFKA_TOPIC = "trip_updates_siri_lite"
KAFKA_CONF = {'bootstrap.servers': 'kafka:9092'}

URL = "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-estimated-timetable"

def delivery_report(err, msg):
    if err is not None:
        print(f"Envoie KO : {err}")
    else:
        print(f"Envoie OKsur {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def fetch_and_push():
    response = requests.get(URL)
    if response.status_code != 200:
        raise Exception(f"Erreur: {response.status_code}")

    root = ET.fromstring(response.content)
    ns = {'siri': 'http://www.siri.org.uk/siri'}
    journeys = root.findall('.//siri:EstimatedVehicleJourney', ns)

    trips = []

    production_date = datetime.now()

    for journey in journeys:
        vj_ref = journey.findtext('.//siri:VehicleJourneyRef', default="N/A", namespaces=ns)
        date = journey.findtext('.//siri:DataFrameRef', default="N/A", namespaces=ns)
        dated_vj_ref = journey.findtext('.//siri:DatedVehicleJourneyRef', default="N/A", namespaces=ns)
        
        unique_id = f"{date}_{dated_vj_ref}"

        train_num = journey.findtext('.//siri:TrainNumberRef', default="N/A", namespaces=ns)
        line = journey.findtext('.//siri:PublishedLineName', default="N/A", namespaces=ns)
        direction = journey.findtext('.//siri:DirectionRef', default="N/A", namespaces=ns)
        
        origin_name = journey.findtext('.//siri:OriginName', default="N/A", namespaces=ns)
        departure_time = journey.findtext('.//siri:OriginAimedDepartureTime', default="N/A", namespaces=ns)
        
        dest_name = journey.findtext('.//siri:DestinationName', default="N/A", namespaces=ns)
        arrival_time = journey.findtext('.//siri:DestinationAimedArrivalTime', default="N/A", namespaces=ns)

        trip = {
                "trip_id": unique_id,
                "train": train_num,
                "origin_name": origin_name,
                "departure_time":departure_time,
                "dest_name":dest_name,
                "arrival_time":arrival_time,
                "stops" : [],
                "production_date": str(production_date)
            }
        counter = 0
        for call in journey.findall('.//siri:RecordedCall', ns):
            stop_name = call.findtext('siri:StopPointName', default="", namespaces=ns)
            aimed_arrival = call.findtext('siri:AimedArrivalTime', default="", namespaces=ns)
            expected_arrival = call.findtext('siri:ExpectedArrivalTime', default="", namespaces=ns)
            aimed_departure = call.findtext('siri:AimedDepartureTime', default="", namespaces=ns)
            expected_departure = call.findtext('siri:ExpectedDepartureTime', default="", namespaces=ns)
            
            trip['stops'].append({
                        "trip_id":unique_id,
                        "stop_name":stop_name,
                        "expected_arrival":expected_arrival,
                        "aimed_arrival":aimed_arrival,
                        "expected_departure":expected_departure,
                        "aimed_departure":aimed_departure,
                        "is_starting_point": 1 if counter==0 else 0,
                        "is_terminus": 1 if counter==len(journey.findall('.//siri:RecordedCall', ns))-1 else 0,
                        "production_date": str(production_date)
            })
            counter+=1
        trips.append(trip)
        # print(trip)
        print("\n" + "-"*60 + "\n")

    with open("/opt/airflow/logs/trips_update_siri_lite.json", 'wb') as f:
        f.write(json.dumps(trips, ensure_ascii=False, indent=2).encode('utf-8'))
    
    producer = Producer(KAFKA_CONF)
    for trip in trips:
        producer.produce(KAFKA_TOPIC, value=json.dumps(trip), callback=delivery_report)
        counter+=1
        print(counter)

    producer.flush()
# 
default_args = {
    'owner': 'sncf-data',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'collect_gtfs_rt_sncf_siri_lite_version',
    default_args=default_args,
    schedule_interval='*/2 * * * *',
    catchup=False,
    tags=[]
) as dag:
    
    collect_task = PythonOperator(
        task_id='fetch_and_push',
        python_callable=fetch_and_push,
        trigger_rule=TriggerRule.DUMMY ###
    )

    collect_task
