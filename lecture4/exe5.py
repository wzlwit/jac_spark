
import mysql.connector
#Install connector using: sudo pip install mysql-connector-python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 2)

lines = ssc.socketTextStream("localhost", 9999)

def saveResult(part):
  hostname="localhost"
  usr="root"
  pwd="admin"
  db="blockchain"
  tbl="t1"
  sql = "INSERT INTO t1 (c1) VALUES (%s)"
  import mysql.connector
  mydb = mysql.connector.connect(host=hostname, user=usr, password=pwd, database=db)
  mycursor = mydb.cursor()
  for record in part:
    val = (record,)
    mycursor.execute(sql, val)
  mydb.commit()
  mydb.close()

lines.foreachRDD(lambda x: x.foreachPartition(saveResult))

ssc.start()             # Start the computation
ssc.awaitTermination()
