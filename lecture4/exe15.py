from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split as _split
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import window

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option('includeTimestamp', 'true') \
    .load()

productSchema = StructType().add("product_id", "integer").add("name", "string").add("added_dt","string").add("deactivated_dt","string")

productDf = spark.read.csv("./product.csv", mode="DROPMALFORMED", schema=productSchema)

splittedClms = _split(lines.value, ",")
combined_df = lines.withColumn("tx_id",splittedClms.getItem(0).cast("integer")) \
    .withColumn("product_id",splittedClms.getItem(1).cast("integer")) \
    .withColumn("qty",splittedClms.getItem(2).cast("integer")) \
    .withColumn("amt",splittedClms.getItem(3).cast("integer")) \
    .withColumn("day_dt",splittedClms.getItem(4).cast("string")) 

streamingDf = combined_df.select("tx_id", "product_id", "qty", "amt", "day_dt", "timestamp")

joinedDf = streamingDf.join(productDf, "product_id").select("tx_id","product_id", "name", "qty", "amt", "timestamp")  # inner equi-join with a static DF

aggDf = joinedDf.groupBy(window(joinedDf.timestamp, "2 minutes", "1 minutes"), joinedDf.product_id, joinedDf.name).agg(_sum("qty"), _sum("amt")).sort(joinedDf.product_id)
# TODO: .sort(joinedDf.columns[1])

# What is 2 minutes and 1 minutes here? a: time_window and refresh frequence (every 1 minute)

query = aggDf \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false").start()

# 'complete' mean all the rows of input since start point, even it is not changed

query.awaitTermination()

#You need to start the python server by running generate_tx.py script