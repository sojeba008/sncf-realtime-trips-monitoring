from confluent_kafka import Consumer
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import json
import os

# -- Delta Lake setup
builder = SparkSession.builder \
    .appName("Consumme to Delta App") \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1,io.delta:delta-core_2.12:2.2.0,io.delta:delta-sharing-spark_2.12:0.6.2') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -- Kafka consumer config
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'gtfs-delta-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['trip_updates_gtfs'])

def parse_feed_to_rows(entity):
    rows = []
    if entity.HasField("trip_update"):
        trip = entity.trip_update.trip
        trip_id = trip.trip_id
        start_date = trip.start_date
        timestamp = entity.trip_update.timestamp  # POSIX timestamp

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


def parse_trip_update(entity_bytes):
    feed_entity = gtfs_realtime_pb2.FeedEntity()
    feed_entity.ParseFromString(entity_bytes)

    # trip = feed_entity.trip_update.trip
    data = parse_feed_to_rows(feed_entity)
    print("HAA")
    # print(data)

    return data

    return {
        # "trip_id": trip.trip_id,
        # "stop_id": trip.stop_id,
        # "StopPoint": trip.StopPoint,
        # "arrival_time": trip.arrival_time,
        # "departure_time": trip.departure_time,
        # "departure_delay": trip.departure_delay,
        # "start_date": trip.start_date,
        # "update_timestamp": trip.update_timestamp,
        # "vehicle_id": vehicle.id if vehicle else None,
    }


DELTA_PATH = "/delta/sncf_trip_updates"


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    try:
        parsed = parse_trip_update(msg.value())
        df = spark.createDataFrame(parsed)
        df.write.format("delta").mode("append").save(DELTA_PATH)
        print(parsed)

    except Exception as e:
        print("Erreur parsing ou écriture :", e)

consumer.close()
