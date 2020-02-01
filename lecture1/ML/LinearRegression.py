from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data = spark.read.csv("mlinput.csv", inferSchema=True)
data.printSchema()
feature_columns = data.columns[1:]

# Need to install numpy if haven't. 
# Command: pip install numpy
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=feature_columns,outputCol="features")
transformed_data = assembler.transform(data)

train, test = transformed_data.randomSplit([0.8, 0.2])

from pyspark.ml.regression import LinearRegression
linearregression = LinearRegression(featuresCol="features", labelCol="_c0")
model = linearregression.fit(train)

predictions = model.transform(test)

predictions.show()