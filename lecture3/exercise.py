s= spark.createDataFrame([Row("s1",'Montreal'),Row('s2','Toronto')],['store_num','locaiton'])
p=spark.createDataFrame([Row('p1','banana'),Row('p2','apple')],['product_num','name'])
t=spark.createDataFrame([Row('t1','s1','p1',3),Row('t2','s2','p2',4)],['transaction_num','store_num','product_num','amount'])

sp = s.crossJoin(p)
sp.join(t, on=['store_num', 'product_num'], how='left').select("location","name",F.when(t.transaction_num.isNull(), "NO").otherwise("YES").alias("status")).show()


