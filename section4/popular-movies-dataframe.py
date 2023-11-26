from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import os
import codecs

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Load up movie data as dataframe
file_path = os.path.join(os.path.abspath(os.curdir),"dataset/ml-100k/u.data")
movie_names_file = os.path.join(os.path.abspath(os.curdir),"dataset/ml-100k/u.item")

# Note the u.item give the mapping between movie id and movie name
#      we could get that in a dataframe and run a join (that actually
#      makes sense here) but we want to test the BROADCAST feature
#      that basically sends information (as a dict) to all the hosts to
#      make it available (note it's only sent once with broadcast)
#
# spark context broadcast
# sc.broadcast() to ship whatever you want to the execution nodes and keep it there
# user the .value( to get the object back
# # the object once sent can be used for anything.. map, UDF (user defined functions.. etc..)

def loadMovieNames()->dict:
    movieNames = {}
    with codecs.open(movie_names_file, "r",
                     encoding='ISO-8859-1',
                     errors = 'ignore' ) as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#
# now broadcast it by sending the UDF function
nameDict = spark.sparkContext.broadcast(loadMovieNames())


# Create schema when reading u.data
schema = StructType([
                     StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)
                     ])

#
# movies DF
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file://" + file_path )

# Some SQL-style magic to sort all movies by popularity in one line
# func.desc.count is another way to sort in descending order
# 
# topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))
#
# slightly changed because now we want just the count and replace the Id with the movie name
moviesDF = moviesDF.groupBy("movieID").count()

#
# let's now create a second UDF function
# that gets the movie name from the broadcasted object
def lookupName(movieID:int)->str:
    # note: it's important to use: .value to work on the object
    return nameDict.value[movieID]
#
# NOW LET's REGISTER THE FUNCTION
# now we can use it in sparkSQL
lookupNameUDF = func.udf(lookupName)
#
# and use it to create a new column
movies_with_names = moviesDF.withColumn("movieTitle",lookupNameUDF(func.col("movieID")))

movies_with_names.show()
# Grab the top 10
sorted_movies_with_names = movies_with_names.orderBy(func.desc("count"))
sorted_movies_with_names.show(10)

# Stop the session
spark.stop()
