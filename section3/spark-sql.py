from logging import INFO
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession (instead of a spark context)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    #
    # note that ID,name,age,numFriends
    # become the column names
    return Row(ID=int(fields[0]), 
               name=str(fields[1].encode("utf-8")), 
               age=int(fields[2]), 
               numFriends=int(fields[3]))
#
# note we access the sparkcontext to use the methods
# and load an unstructured data file (fields are not typed)
lines = spark.sparkContext.textFile("dataset/fakefriends.csv")
spark.sparkContext.setLogLevel("WARN")
#
# because a dataframe is a collection of rows I need to create the rows first
# using an rdd (people)
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table from the RDD.
# Note that we cache it to keep it in memory
schemaPeople = spark.createDataFrame(people).cache()
#
# we crate a view named people out of the dataframe 
# (basically that's the name of the table where the data goes)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
# I can run an sql query based on the column names defined above
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
# No we are not getting the dataframe back.. just show the first lines
schemaPeople.groupBy("age").count().orderBy("age").show()

#
# you need to explicitely stop it when you're done
spark.stop()