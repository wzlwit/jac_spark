
# sudo apt update
# sudo apt install mysql-server

import mysql.connector
#Install connector using: sudo python -m pip install mysql-connector-python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9999)

def saveResult(part):
  hostname="localhost"
  usr="root"
  pwd="root"
  db="blockchain"
  tbl="t1"
  sql = "INSERT INTO t1 (c1) VALUES (%s)"
  import mysql.connector    # !!!: for each executro
  mydb = mysql.connector.connect(host=hostname, user=usr, password=pwd, database=db)
  mycursor = mydb.cursor()    # * handler
  for record in part:
    val = (record,)
    mycursor.execute(sql, val)
  mydb.commit()
  mydb.close()

lines.foreachRDD(lambda x: x.foreachPartition(saveResult))    # send function 'saveResult' to each executor of RDD

ssc.start()             # Start the computation
ssc.awaitTermination()
