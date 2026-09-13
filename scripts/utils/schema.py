from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

STOP_TIME_EVENT = StructType([
    StructField("delay", IntegerType()),
    StructField("time", StringType()),
    StructField("scheduledTime", StringType()),
    StructField("uncertainty", IntegerType()),
])

TRIP_UPDATE_SCHEMA = StructType([
    StructField("trip", StructType([
        StructField("tripId", StringType()),
        StructField("routeId", StringType()),
        StructField("directionId", IntegerType()),
        StructField("startTime", StringType()),
        StructField("startDate", StringType()),
        StructField("scheduleRelationship", StringType()),
    ])),
    StructField("vehicle", StructType([
        StructField("id", StringType()),
        StructField("label", StringType()),
        StructField("licensePlate", StringType()),
        StructField("wheelchairAccessible", StringType()),
    ])),
    StructField("stopTimeUpdate", ArrayType(StructType([
        StructField("stopSequence", IntegerType()),
        StructField("stopId", StringType()),
        StructField("arrival", STOP_TIME_EVENT),
        StructField("departure", STOP_TIME_EVENT),
        StructField("departureOccupancyStatus", StringType()),
        StructField("scheduleRelationship", StringType()),
        StructField("stopTimeProperties", StructType([
            StructField("assignedStopId", StringType()),
            StructField("stopHeadsign", StringType()),
        ])),
    ]))),
    StructField("timestamp", StringType()),
    StructField("delay", IntegerType()),
])