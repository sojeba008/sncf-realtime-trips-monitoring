from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer
import json
from airflow.utils.trigger_rule import TriggerRule

KAFKA_TOPIC = "trip_updates_gtfs"
KAFKA_CONF = {'bootstrap.servers': 'kafka:9092'}  

URL_GTFS_RT_TU = "https://proxy.transport.data.gouv.fr/resource/sncf-all-gtfs-rt-trip-updates"

def delivery_report(err, msg):
    if err is not None:
        print(f"Envoie KO : {err}")
    else:
        print(f"Envoie OKsur {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def parse_feed_to_rows(feed):
    rows = []

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip = entity.trip_update.trip
            trip_id = trip.trip_id
            start_date = trip.start_date
            timestamp = entity.trip_update.timestamp 

            for stu in entity.trip_update.stop_time_update:
                row = {
                    "trip_id": trip_id,
                    "stop_id": stu.stop_id,
                    "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                    "departure_time": stu.departure.time if stu.HasField("departure") else None,
                    "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                    "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                    "start_date": start_date,
                    "update_timestamp": timestamp
                }
                rows.append(row)

    return rows


def fetch_and_push_gtfs_rt():
    response = requests.get(URL_GTFS_RT_TU)
    if response.status_code != 200:
        raise Exception(f"Erreur de récupération : {response.status_code}")

    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    producer = Producer(KAFKA_CONF)
    print("AJIME")

    feed_rows = parse_feed_to_rows(feed)
    # print(feed_rows)


    with open("/opt/airflow/logs/trips_update_gtfs.json", 'wb') as f:
        f.write(str(feed_rows).encode('utf-8'))
    
    counter = 0
    # producer.produce(KAFKA_TOPIC, value=str(feed_rows).encode('utf-8'))
    # counter = 0
    # for row in feed_rows:
    #     producer.produce(KAFKA_TOPIC, value=json.dumps(row).encode('utf-8'), callback=delivery_report)
    #     counter+=1
    #     print(counter)

    for entity in feed.entity:
        producer.produce(KAFKA_TOPIC, value=entity.SerializeToString(), callback=delivery_report)
        counter+=1
        print(counter)
        # print(entity.SerializeToString())

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
    'collect_gtfs_rt_sncf',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=[]
) as dag:
    
    collect_task = PythonOperator(
        task_id='fetch_and_push_gtfs_rt',
        python_callable=fetch_and_push_gtfs_rt,
        trigger_rule=TriggerRule.DUMMY
    )

    collect_task
