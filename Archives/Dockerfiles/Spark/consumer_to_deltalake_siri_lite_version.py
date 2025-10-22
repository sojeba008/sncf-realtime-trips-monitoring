from confluent_kafka import Consumer
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
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
consumer.subscribe(['trip_updates_siri_lite'])

TRIPS_DELTA_PATH="/delta/sncf_trips"
STOPS_DELTA_PATH="/delta/sncf_trip_stops"
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    try:
        
        value = msg.value().decode('utf-8')
        trip_data = json.loads(value)
        # with open("/app/trip_data.json", 'wb') as f:
        #     f.write(json.dumps(trip_data, ensure_ascii=False, indent=2).encode('utf-8'))
        
        trip_df = spark.createDataFrame([{
            "trip_id": trip_data["trip_id"],
            "train": trip_data["train"],
            "origin_name": trip_data["origin_name"],
            "departure_time": trip_data["departure_time"],
            "dest_name": trip_data["dest_name"],
            "arrival_time": trip_data["arrival_time"],
            "production_date": trip_data['production_date']
        }])

        trip_df.write.format("delta").mode("append").save(TRIPS_DELTA_PATH)
        # if not DeltaTable.isDeltaTable(spark, TRIPS_DELTA_PATH):
        #     trip_df.write.format("delta").save(TRIPS_DELTA_PATH)
        # else:
        #     delta_table = DeltaTable.forPath(spark, TRIPS_DELTA_PATH)

        #     delta_table.alias("target").merge(
        #         trip_df.alias("source"),
        #         "target.trip_id = source.trip_id"
        #     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"OK : {trip_data['trip_id']}")

        
        stops = trip_data.get("stops", [])
        # for stop in stops:
        #     stop["trip_id"] = trip_data["trip_id"]

        if stops:
            stops_df = spark.createDataFrame(stops)
            stops_df.write.format("delta").mode("append").save(STOPS_DELTA_PATH)
            # if not DeltaTable.isDeltaTable(spark, STOPS_DELTA_PATH):
            #     stops_df.write.format("delta").save(STOPS_DELTA_PATH)
            # else:
            #     delta_stops = DeltaTable.forPath(spark, STOPS_DELTA_PATH)

            #     delta_stops.alias("target").merge(
            #         stops_df.alias("source"),
            #         """
            #         target.trip_id = source.trip_id AND
            #         target.stop_name = source.stop_name
            #         """
            #     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"{len(stops)} stops ajoutés pour {trip_data['trip_id']}")
        else:
            print("Aucun arrêt à écrire")

    except Exception as e:
        print("Erreur parsing ou écriture :", e)

consumer.close()
