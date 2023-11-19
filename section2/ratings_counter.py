#
# run with python3 ratings_counter.py
# or
# spark-submit ratings_counter.py

from pyspark import SparkConf, SparkContext
import collections

#
# setMaster("local") -> runs on local machine, single thread/cpu
# setAppName -> used to identify the app in the spark UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#
# we crate a spark context (or reuse an existing one if it is still alive)
sc = SparkContext.getOrCreate(conf = conf)

#
# this will read from file but you can use different sources
# implicitely here is assumes file:///
# but if we use file:/// you need the absolute file path
#
# lines is an rdd: resilient distributed dataset (RDD)
lines = sc.textFile("dataset/ml-100k/u.data")
#
# data format is lines like the following and it takes every line indipendently:
# <user_id> <film_id> <rate> <timestamp>
# 196	242	3	881250949
# 186	302	3	891717742
# 22	377	1	878887116
# 244	51	2	880606923
#
# ratings is a new RDD
ratings = lines.map(lambda x: x.split()[2])  # parse them all and the the 3rd value (i.e. the ratings)
print(f"type of lines:  {type(lines)}")
print(f"type of ratings:  {type(ratings)}")
result = ratings.countByValue()   #<- this is an action on the RDD and it does not return an rdd but a normal python object

print(f"type of result: {type(result)}")
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))