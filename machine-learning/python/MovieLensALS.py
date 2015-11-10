#!/usr/bin/env python

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

import os
import sys

def parseRating(line):
    """
    Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
    """
    fields = line.strip().split("::")
    return long(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))

def parseMovie(line):
    """
    Parses a movie record in MovieLens format movieId::movieTitle .
    """
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

def loadRatings(ratingsFile):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print "File %s does not exist." % ratingsFile
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print "No ratings provided."
        sys.exit(1)
    else:
        return ratings

def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


def getRecommendation(arg0, arg1, arg2):
    # set up environment
    conf = SparkConf() \
      .setAppName("MovieLensALS") \
      .set("spark.executor.memory", "2g") \
      .set("spark.app.id", '2015Project')
    sc = SparkContext(conf=conf)

    # load personal ratings
    myRatings = loadRatings(arg2)
    myRatingsRDD = sc.parallelize(myRatings, 1)
    myRatings1 = myRatings 
    # myRatingsRDD1 = myRatingsRDD

    # load ratings and movie titles

    movieLensHomeDir = arg1

    # ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
    ratings = sc.textFile(join(movieLensHomeDir, "ratings.dat")).map(parseRating)

    # movies is an RDD of (movieId, movieTitle)
    movies = dict(sc.textFile(join(movieLensHomeDir, "movies.dat")).map(parseMovie).collect())

    # your code here
    # myRatings = ratings.count()
    # myUsers = ratings.values().map(lambda r: r[0]).distinct().count()
    # myMovies = ratings.values().map(lambda r: r[1]).distinct().count()
    # myRDDCount = myRatingsRDD.count()

    modelPath = "/home/hduser/Downloads/spark-training-master/machine-learning/python/model"
    if not os.path.isdir(modelPath):
        numPartitions = 4
        training = ratings.filter(lambda x: x[0] < 6) \
          .values() \
          .union(myRatingsRDD) \
          .repartition(numPartitions) \
          .cache()

        validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8) \
          .values() \
          .repartition(numPartitions) \
          .cache()

        test = ratings.filter(lambda x: x[0] >= 8).values().cache()

        numValidation = validation.count()
        # numTraining = training.count()
        # numTest = test.count()
        # print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)
        # # print "Got %d ratings from %d users on %d movies." % (myRatings, myUsers, myMovies)
        # print "Got %d ratings from %d users on %d movies. rdd %d, train %d" % (myRatings, myUsers, myMovies, myRDDCount, training.count())
        
    # 3
        ranks = [8, 12]
        lambdas = [1.0, 10.0]
        numIters = [10, 20]
        
        bestModel = None
        bestValidationRmse = float("inf")
        bestRank = 0
        bestLambda = -1.0
        bestNumIter = -1

        for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
            model = ALS.train(training, rank, numIter, lmbda)
            validationRmse = computeRmse(model, validation, numValidation)
            print "RMSE (validation) = %f for the model trained with " % validationRmse + \
                  "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter)
            if (validationRmse < bestValidationRmse):
                bestModel = model
                bestValidationRmse = validationRmse
                bestRank = rank
                bestLambda = lmbda
                bestNumIter = numIter

        bestModel.save(sc, modelPath)
    else:
        bestModel = MatrixFactorizationModel.load(sc, modelPath)
    # testRmse = computeRmse(bestModel, test, numTest)
    # evaluate the best model on the test set
    # print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
    #   + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)
# 4
    myRatedMovieIds = set([x[1] for x in myRatings1])
    candidates = sc.parallelize([m for m in movies if m not in myRatedMovieIds])
    predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:10]

    outputBuffer =  "Movies recommended for you:\n"
    for i in xrange(len(recommendations)):
        outputBuffer += ("%2d: %s\n" % (i + 1, movies[recommendations[i][1]])).encode('ascii', 'ignore')
    sc.stop()
    return outputBuffer