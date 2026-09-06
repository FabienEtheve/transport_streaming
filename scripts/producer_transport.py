from google.transit import gtfs_realtime_pb2
import requests
import time
from kafka import KafkaProducer
import json
from google.protobuf.json_format import MessageToDict

URL = "https://www.data.gouv.fr/api/1/datasets/r/7dd97f96-2fb2-4647-8498-ef234ed0faaa"
INTERVAL = 15

starttime = time.monotonic()
last_timestamp = None
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: v.SerializeToString(),
    key_serializer=lambda k: k.encode("utf-8"),
)

while True:
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        if feed.header.timestamp != last_timestamp:
            last_timestamp = feed.header.timestamp
            print(f"--- feed {feed.header.timestamp}, {len(feed.entity)} entites")
            for entity in feed.entity:
                if entity.HasField("trip_update"):
                    
                    producer.send("transport", key=entity.id, value=entity.trip_update)
            producer.flush()
            
        else:
            print("snapshot inchange, skip")

    except requests.RequestException as e:
        print(f"erreur reseau: {e}")

    time.sleep(INTERVAL - ((time.monotonic() - starttime) % INTERVAL))