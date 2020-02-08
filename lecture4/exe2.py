import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if len(sys.argv) < 2:
  print "Please pass file dir as parameter"
  sys.exit(0)

dataSourceDir = sys.argv[1]
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)
lines = ssc.textFileStream(dataSourceDir)   #* only read new files, not modified
lines.pprint()
ssc.start()
ssc.awaitTermination()

# spark-submit exe2.py /home/student/jac_spark/lecture4/dStr
