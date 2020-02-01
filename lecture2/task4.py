#* Task 4: Classification Algorithm: Text classification without pipeline

#* Objective: Classify hotel review text as positive or negative using LogisticRegression model

#Prepared input dataset
training = spark.createDataFrame([
    ("We had a perfectly pleasant stay here in December.", 1),
    ("Stayed at this hotel again and it was as good as last year. Great service, perfect location and very clean.", 1),
    ("Very negative experience when trying to get a refund from my travelocity booking that was cancelled due to a weather delay .", 0),
    ("Staff is very green young, restaurant is nice but forget it worst service ever. breakfast, burned toasts, cold eggs waited forever to be seated and for the food. ", 0)
], ["review", "label"])

#Tokenize sentences 
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")

#Generate hash for each word
from pyspark.ml.feature import HashingTF
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

#Create LogisticRegression instance
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10)

#Train the model
tkns = tokenizer.transform(training) 
htf = hashingTF.transform(tkns)
model = lr.fit(htf)

#Predict
test = spark.createDataFrame([
    ("Very negative experience. Not impressed. I won’t be staying here again and I recommend you don’t either.",),
    ("Great service, perfect location and very clean.",),
    ("The staff were all excellent. Super pleasant and courteous.",),
    ("worst service ever. waited forever to be seated and for the food.",)
], ["review"])

test_tkns = tokenizer.transform(test) 
test_htf = hashingTF.transform(test_tkns)
prediction = model.transform(test_htf)

prediction.show()


