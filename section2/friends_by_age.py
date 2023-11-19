from pyspark import SparkConf, SparkContext
import pprint
#
# run with python3 friends_by_age.py
# or
# spark-submit friends_by_age.py
# rdd can be used to store key values pairs
# one of the most usefult things would be to reduce by keys:
#
#    rdd.reduceByKey(lambda ...)
#    rdd.groupByKey()
#    rdd.sortByKey(lambda ...)
#    rdd.keys(), rdd.values() -> creates new rdd with keys or values
#
# also functions like join, rightOuterJoin, leftOuterJoin, cogroup, subtractbyKey
#
# IMPORTANT NOTE: if you are not going to modify your keys use
#    mapValues() and flatMapValues() instead of using map(), flatMap()
# it allows more efficient processing
# Notes:
#  mapValues() crate a new element for every old element (using a lambda provided)
#  flatMapValues() can create multiple lements for every original element
#
# fake dataset
# 0,Will,33,385
# 1,Jean-Luc,26,2
# 2,Hugh,55,221
# 3,Deanna,40,465
# 4,Quark,68,21
# [...]


def parseLine(line:str) -> (int,int):
    """
    """
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age,num_friends)

conf = SparkConf().setMaster("local").setAppName("AverageFriends")
#
# we crate a spark context (or reuse an existing one if it is still alive)
sc = SparkContext.getOrCreate(conf = conf)

lines = sc.textFile("dataset/fakefriends.csv")

rdd = lines.map(parseLine)
# every line is parsed and a new rdd is created with age and num_friends
# because the function returns 2 values it is considered a key value pair



# rdd.mapValues(lambda x: (x, 1))
# every value (only considering the values here) is going to map all X values to a pair tuple where we have 
# X and 1 (the reason is that at the end we want to know how many X we have and we can sum the 1s to get there)
# so 
# (33,385) -> (33,(385,1))  # Note i still have the key (it's untouched)
# (26,2)   -> (26, (2,1))
# (33,18)  -> (33, (18,1))
# 
# reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])
# combines entries with the same key together using lambda
# it parses the full rdd
# and when it finds a key that it knows add the values using lambda
# using the example above 
# (33,(385,1))
# (26, (2,1))
# (33, (18,1))
# when finds the second 33
# it just adds the values and return
# (33,(403, 2))  # 385+18=403, 1 + 1 = 2
# at the end the keys will be unique
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))   #<- first action is reduce by Key
#
# finally for each (unique key), return the average
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect()                      # <- second action is collect
for result in results:
    print(result)
