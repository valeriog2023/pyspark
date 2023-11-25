import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
#
# In this case the dataset has a header
# we don't create the context and load the file in an rdd
# but we use directly the read option
# userID,name,age,friends
# 0,Will,33,385
# 1,Jean-Luc,26,2
# 2,Hugh,55,221
# 3,Deanna,40,465
#
# we also ask spark to infer a schema
people = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("dataset/fakefriends-header.csv")
print("----------------")
print("people is of type " + str(type(people)))
print("Here is our inferred schema: people.printSchema()")
people.printSchema()
time.sleep(5)

print("Let's display the name column:")
print("people.select('name').show()")
people.select("name").show()
time.sleep(5)

print("Filter out anyone over 21:")
print("people.filter(people.age < 21).show()")
people.filter(people.age < 21).show()
time.sleep(5)

print("Group by age")
print("people.groupBy('age').count().show()")
people.groupBy("age").count().show()
time.sleep(5)

print("Make everyone 10 years older (just show.. no real change):")
print("people.select(people.name, people.age + 10).show(50)")
people.select(people.name, people.age + 10).show(50)
time.sleep(5)

spark.stop()

