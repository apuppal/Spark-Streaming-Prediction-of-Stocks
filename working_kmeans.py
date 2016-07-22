from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from numpy import *
from math import sqrt


#########

ssc = StreamingContext(sc, 10)
test_stream = ssc.socketTextStream("localhost", 9997)
data = sc.textFile("streamapp/working/train.txt")
pData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
model = KMeans.train(pData, 5, maxIterations=10,
         runs=10, initializationMode="random")
parsedData = test_stream.map(lambda line: array([float(x) for x in line.split(' ')]))
test_prediction = parsedData.map(lambda test: model.predict(test))
test_prediction.pprint()


ssc.start()             
ssc.awaitTermination()  