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
#Create a streaming context so that you can accept data real time into the program
ssc = StreamingContext(sc, 10)
#Create a Text stream to take the data from test file, it comes in the form of DStream
test_stream = ssc.socketTextStream("localhost", 9997)
#Load the training data set
data = sc.textFile("/Users/apple/Documents/uconn/sparktraining/streamapp/streamapp/streamapp/streamapp/working/train.txt")
#Parse the values that are obtained from the training data set into Sparsed Vector
pData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
#Prepare a KMeans model based on training dataset
model = KMeans.train(pData, 5, maxIterations=10,
         runs=10, initializationMode="random")
#Parse the test data into ParsedVectors.
parsedData = test_stream.map(lambda line: array([float(x) for x in line.split(' ')]))
#Send the DStream of Parsed Vector for the test data through the map function to predict the cluster to which it belongs
test_prediction = parsedData.map(lambda test: model.predict(test))
#Use pprint function to print the output of test_prediction
test_prediction.pprint()

#Start the streaming context
ssc.start()             
ssc.awaitTermination()  