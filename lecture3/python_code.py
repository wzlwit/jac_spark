from pyspark.sql import SparkSession
if __name__ == "__main__":
  #Initialize the spark session
  spark = SparkSession \
        .builder \
        .appName("pyspark script job") \
        .getOrCreate()

  spark.conf.set("spark.driver.memory","1.5G")
  print "####################" + spark.conf.get("spark.driver.memory") + "####################"
  spark.stop()
