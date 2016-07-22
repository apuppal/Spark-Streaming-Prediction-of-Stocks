import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 

val ssc = new StreamingContext(sc,Seconds(10))
val trainingData = ssc.socketTextStream("localhost", 9997).map(Vectors.parse)
val testData = ssc.socketTextStream("localhost", 9996).map(LabeledPoint.parse)
val numDimensions = 3
val numClusters = 5
val model = new StreamingKMeans().setK(numClusters).setDecayFactor(1.0).setRandomCenters(numDimensions, 0.0)

 model.trainOn(trainingData)
 model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
 ssc.start()
