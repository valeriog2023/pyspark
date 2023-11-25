from pyspark.sql import SparkSession
from pyspark.sql import functions as func
#
# using sparksql and the firends dataset
# Data is tructured as the following
# <id>,<name>,<age>,<number of friends>
# 0,Will,33,123
# 1,Jey,22,128
# ..
# find the average number of freinds people have by age


spark = SparkSession.builder.appName("SparkSQL_ex1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
#
# we also ask spark to infer a schema (you can provide your own schema)
people = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("dataset/fakefriends-header.csv")


print("----------------")
print("people is of type " + str(type(people)))
print("Here is our inferred schema: people.printSchema()")
people.printSchema()
#
#
print("Grouping data by age: result type: GroupedData")
print("Note you can't show grouped data directly.. only by using an aggregate function")
#
# few way to select  only the 2 columns we care about
# Note that 2 and 3 get the same result and basically add one additional column (compared to 1)
#
# also note that you can use both orderBy("column") or sort("column")
print("-----------------\nGroupe Data by age: count ")
people['age','friends'].groupBy("age").count().orderBy('age').show(10)

print("-----------------\nGroupe Data by age: sum of friends - explicit print of columns")
people.select("age","friends").groupBy("age").sum().sort('age')['age','sum(friends)'].show(10)

friendsByAge = people.select("age","friends")

print("-----------------\nGroupe Data by age: avg of friends")
friendsByAge.groupBy("age").avg().orderBy('age').show(10)

print("-----------------\nGroupe Data by age: avg of friends - using aggregate and rounding")
#
# PySpark provides various aggregation functions, 
# such as sum(), avg(), min(), max(), count(), etc., 
# that can be applied to a column or a group of columns using the agg() 
# method on the DataFrame object.
# you can use different function in different columns
# e.g. aggregated_df = df.agg({"state_id": "avg", "state_id_I": "sum"})
#
# note: you can also define an alias fro the column name
# note that alias is applied inside the agg function
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show(10)
