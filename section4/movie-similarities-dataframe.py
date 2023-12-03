from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

#
# note that .master("local[*]") means use every cpu on the local system
#      remember that if you run on a cluster, you don't want to constraint it to local
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#
# note: data is in the format:
# 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
# 2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
# we only get the first 2 values
movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
#note: this data is in the format
# 196	242	3	881250949
# 186	302	3	891717742
# 22	377	1	878887116
# 244	51	2	880606923
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("dataset/ml-100k/u.item")

#
# Load up movie data as dataset
movies = spark.read \
           .option("sep", "\t") \
              .schema(moviesSchema).csv("dataset/ml-100k/u.data")



def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Compute numerator, denominator and numPairs columns
    # this groups the movie pairs and replaces the individual ratings
    # with
    #  - in the numerator column: the sum of the xy column
    #  - in the denominator column:
    #      - the product of
    #          - square root of the sum of the xx column, times
    #          - square root of the sum of the yy column
    #  - in the numPairs column: the number of entries for the grouped pair (movieA,movieB)
    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
                           func.sum(func.col("xy")).alias("numerator"),
                           (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
                           func.count(func.col("xy")).alias("numPairs")
                          )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    # Note the use of when(<condition>, <expression>).otherwise()
    result = calculateSimilarity \
      .withColumn("score",
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result



# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]








ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
                    .join(ratings.alias("ratings2"),
                          # join based on same user id
                         (func.col("ratings1.userId") == func.col("ratings2.userId")) \
                          # this actually avoids duplicates and entreis with same movieId
                         & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))
                        ).select(func.col("ratings1.movieId").alias("movie1"),
                                 func.col("ratings2.movieId").alias("movie2"),
                                 func.col("ratings1.rating").alias("rating1"),
                                 func.col("ratings2.rating").alias("rating2")
                                 )


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()



if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(
          ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2

        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))

