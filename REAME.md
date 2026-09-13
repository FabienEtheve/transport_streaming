docker exec -it spark-master-transp /opt/spark/bin/spark-submit `
>>   --master spark://spark-master-transp:7077 `
>>   --conf spark.jars.ivy=/tmp/.ivy2 `
>>   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 `
>>   /opt/jobs/consumer_transport.py