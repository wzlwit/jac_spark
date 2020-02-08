from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
# spark.sparkContext.setLogLevel('WARN')

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

df = lines.selectExpr("CAST(value AS STRING)")

query = df \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "test") \
  .option("checkpointLocation", "./checkpoint") \
  .start()

# todo: clear 'checkpoint'folder for debuging

query.awaitTermination()

#How to execute spark job
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 exe12.py

#Create Consumer
#	bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
