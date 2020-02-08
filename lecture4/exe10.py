from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

productSchema = StructType().add("id", "integer").add("name", "string")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test") \
  .load()

keyValDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = keyValDf \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start()

query.awaitTermination()

#How to execute spark job
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 exe10.py

#How to run kafka console producer to produce key value pairs
# bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test --property "parse.key=true" --property "key.separator=:"