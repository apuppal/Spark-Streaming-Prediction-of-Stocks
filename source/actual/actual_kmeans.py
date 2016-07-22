"""This Program takes two data sets Training Data and Test Data, it prepares a cluster based on Training Data 
    on real time and predicts the cluster for the data in Test dataset. The program accepts data from two 
    socket streams for Training and Test Data. The rate of input is one record per second.
"""

from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from numpy import *
from math import sqrt


###############

class StreamingKMeansModel(KMeansModel):
    def __init__(self, clusterCenters, clusterWeights):
        super(StreamingKMeansModel, self).__init__(centers=clusterCenters)
        self._clusterWeights = list(clusterWeights)
    @property
    def clusterWeights(self):
        """Return the cluster weights."""
        return self._clusterWeights
    #@ignore_unicode_prefix
    def update(self, data, decayFactor, timeUnit):
        """Update the centroids, according to data
        :param data: Should be a RDD that represents the new data.
        :param decayFactor: forgetfulness of the previous centroids.
        :param timeUnit: Can be "batches" or "points". If points, then the
                         decay factor is raised to the power of number of new
                         points and if batches, it is used as it is.
        """
        if not isinstance(data, RDD):
            raise TypeError("Data should be of an RDD, got %s." % type(data))
        data = data.map(_convert_to_vector)
        decayFactor = float(decayFactor)
        if timeUnit not in ["batches", "points"]:
            raise ValueError(
                "timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        vectorCenters = [_convert_to_vector(center) for center in self.centers]
        updatedModel = callMLlibFunc(
            "updateStreamingKMeansModel", vectorCenters, self._clusterWeights,
            data, decayFactor, timeUnit)
        self.centers = array(updatedModel[0])
        self._clusterWeights = list(updatedModel[1])
        return self


##############

class StreamingKMeans(object):    
    def __init__(self, k=2, decayFactor=1.0, timeUnit="batches"):
        self._k = k
        self._decayFactor = decayFactor
        if timeUnit not in ["batches", "points"]:
            raise ValueError(
            "timeUnit should be 'batches' or 'points', got %s." % timeUnit)
            self._timeUnit = timeUnit
            self._model = None
    def latestModel(self):
        """Return the latest model"""
        return self._model
    def _validate(self, dstream):
        if self._model is None:
            raise ValueError(
            "Initial centers should be set either by setInitialCenters "
            "or setRandomCenters.")
            if not isinstance(dstream, DStream):
                raise TypeError(
                "Expected dstream to be of type DStream, "
                "got type %s" % type(dstream))
    def setK(self, k):
        """Set number of clusters."""
        self._k = k
        return self
    def setDecayFactor(self, decayFactor):
        """Set decay factor."""
        self._decayFactor = decayFactor
        return self
    def setHalfLife(self, halfLife, timeUnit):
        """
        Set number of batches after which the centroids of that
        particular batch has half the weightage.
        """
        self._timeUnit = timeUnit
        self._decayFactor = exp(log(0.5) / halfLife)
        return self
    def setInitialCenters(self, centers, weights):
        """
        Set initial centers. Should be set before calling trainOn.
        """
        self._model = StreamingKMeansModel(centers, weights)
        return self
    def setRandomCenters(self, dim, weight, seed):
        """
        Set the initial centres to be random samples from
        a gaussian population with constant weights.
        """
        from numpy import random
        from numpy import tile
        rng = random.RandomState(seed)
        clusterCenters = rng.randn(self._k, dim)
        clusterWeights = tile(weight, self._k)
        self._model = StreamingKMeansModel(clusterCenters, clusterWeights)
        return self
    def trainOn(self, dstream):
        """Train the model on the incoming dstream."""
        self._validate(dstream)
    def update(rdd):
        self._model.update(rdd, self._decayFactor, self._timeUnit)
        dstream.foreachRDD(update)
    def predictOn(self, dstream):
        """
        Make predictions on a dstream.
        Returns a transformed dstream object
        """
        self._validate(dstream)
        return dstream.map(lambda x: self._model.predict(x))
    def predictOnValues(self, dstream):
        """
        Make predictions on a keyed dstream.
        Returns a transformed dstream object.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))

#############

def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(',')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
    return LabeledPoint(label, vec)


ssc = StreamingContext(sc, 10)
trainingData = ssc.socketTextStream("localhost", 9997).map(Vectors.parse)
testData = ssc.socketTextStream("localhost", 9996).map(parse)
model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
model.trainOn(trainingData)
strmode = model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features)))
strmode.pprint()
ssc.start()