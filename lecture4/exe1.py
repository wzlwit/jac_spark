import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if len(sys.argv) < 2:
  print "Please pass server ip as parameter"
  sys.exit(0)

sc = SparkContext("local[2]", "NetworkWordCount")   # 'local' is the master ,will be changed to 'yarn' in PROD. in-built Net negociator, at leaset 2 core(processors): listener and reader/executor
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream(sys.argv[1], 9999)   # argv[0] is the name of python file.
lines.pprint()
ssc.start()
ssc.awaitTermination()    # keep CLI open, otherwise CLI will be closed. Though the program is running in background

# spark-submit exe1.py localhost