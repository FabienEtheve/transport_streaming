from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StringType
from utils.validation import ajouter_violations, traiter_batch, mesurer
from utils.schema import TRIP_UPDATE_SCHEMA


def parse_tu(raw):
    from google.transit import gtfs_realtime_pb2
    from google.protobuf.json_format import MessageToJson
    tu = gtfs_realtime_pb2.TripUpdate()
    tu.ParseFromString(raw)
    return MessageToJson(tu)


spark = SparkSession.builder.appName("transport_trip_update").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

parse_udf = spark.udf.register("parse_tu", parse_tu, StringType())

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "broker:9092")
      .option("subscribe", "transport_trip_update")
      .option("startingOffsets", "latest")
      .load())

parsed = df.select(
    col("timestamp").alias("kafka_ts"),
    from_json(parse_udf(col("value")), TRIP_UPDATE_SCHEMA).alias("tu"),
)

flat = parsed.select(
    "kafka_ts",
    col("tu.trip.tripId").alias("trip_id"),
    col("tu.trip.routeId").alias("route_id"),
    col("tu.trip.directionId").alias("direction_id"),
    col("tu.trip.startDate").alias("start_date"),
    col("tu.trip.startTime").alias("start_time"),
    col("tu.trip.scheduleRelationship").alias("trip_schedule_rel"),
    col("tu.vehicle.id").alias("vehicle_id"),
    col("tu.vehicle.label").alias("vehicle_label"),
    col("tu.timestamp").cast("long").alias("tu_timestamp"),
    col("tu.delay").alias("trip_delay"),
    col("stu.arrival.scheduledTime").cast("long").alias("arrival_scheduled_time"),
    col("stu.departure.scheduledTime").cast("long").alias("departure_scheduled_time"),
    col("stu.departureOccupancyStatus").alias("departure_occupancy_status"),
    col("stu.stopTimeProperties.assignedStopId").alias("assigned_stop_id"),
    col("stu.stopTimeProperties.stopHeadsign").alias("stop_headsign"),
    explode("tu.stopTimeUpdate").alias("stu"),
).select(
    "kafka_ts", "trip_id", "route_id", "direction_id",
    "start_date", "start_time", "trip_schedule_rel",
    "vehicle_id", "vehicle_label", "tu_timestamp", "trip_delay",
    col("stu.stopSequence").alias("stop_sequence"),
    col("stu.stopId").alias("stop_id"),
    col("stu.scheduleRelationship").alias("stop_schedule_rel"),
    col("stu.arrival.delay").alias("arrival_delay"),
    col("stu.arrival.time").cast("long").alias("arrival_time"),
    col("stu.arrival.uncertainty").alias("arrival_uncertainty"),
    col("stu.departure.delay").alias("departure_delay"),
    col("stu.departure.time").cast("long").alias("departure_time"),
    col("stu.departure.uncertainty").alias("departure_uncertainty"),
    col("stu.arrival.scheduledTime").cast("long").alias("arrival_scheduled_time"),
    col("stu.departure.scheduledTime").cast("long").alias("departure_scheduled_time"),
    col("stu.departureOccupancyStatus").alias("departure_occupancy_status"),
    col("stu.stopTimeProperties.assignedStopId").alias("assigned_stop_id"),
    col("stu.stopTimeProperties.stopHeadsign").alias("stop_headsign"),
)

query = (flat.writeStream
         .foreachBatch(traiter_batch)
         .option("checkpointLocation", "/opt/jobs/checkpoint")
         .trigger(processingTime="1 minute")
         .start())

query.awaitTermination()