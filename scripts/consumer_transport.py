from google.transit import gtfs_realtime_pb2
from kafka import KafkaConsumer

def deserialize(raw):
    tu = gtfs_realtime_pb2.TripUpdate()
    tu.ParseFromString(raw)
    return tu

consumer = KafkaConsumer(
    "transport",
    bootstrap_servers="localhost:29092",
    group_id="transport-debug",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=deserialize,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)

try:
    for message in consumer:
        print(f"[{message.partition}:{message.offset}] {message.key}")
        print(message.value)
except KeyboardInterrupt:
    print("Stopping")
finally:
    consumer.close()