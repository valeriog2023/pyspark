from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([ 
                     StructField("id", IntegerType(), True), 
                     StructField("name", StringType(), True)
                     ])

# the format of the name data set is
# 1 "24-HOUR MAN/EMMANUEL"
# 2 "3-D MAN/CHARLES CHAN"
# 3 "4-D MAN/MERCURIO"
# 4 "8-BALL/"
# so it is considered a csv file with a blank space as separator
names = spark.read.schema(schema).option("sep", " ").csv("dataset/Marvel-names.txt")
#
# the schema in this case is
# 5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456 
# 5989 4080 4264 4446 3779 2430 2297 6169 3530 3272 4282 6432 2548 4140 185 105 3878 2429 1334 4595 2767 3956 3877 4776 4946 3407 128 269 5775 5121 481 5516 4758 4053 1044 1602 3889 1535 6038 533 3986 
# 5982 217 595 1194 3308 2940 1815 794 1503 5197 859 5096 6039 2664 651 2244 528 284 1449 1097 1172 1092 108 3405 5204 387 4607 4545 3705 4930 1805 4712 4404 247 4754 4427 1845 536 5795 5978 533 3984 6056 
# Note the read text will put everything in a colum called value so we just split it after
#      don't really bother to create a schema here
lines = spark.read.text("dataset/Marvel-graph.txt")

# Here we:
# add a colum called "id" to the lines df
#     the value is given by splitting the value colum of the original df
#     trimming it and assign the first element: func.split(func.trim(func.col("value")), " ")[0])
# add a column called "connections" (note the withColumn is nested here)
#    the value is given by the number of connections (i.e. len of split/values - 1)
#    via func.size(func.split(func.trim(func.col("value")), " ")) - 1)
# finally we group based on id (this is because some heros id can appear in multiple lines)
#    and use the sum function, the grouped by column is also renamed connections
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# connections.show(10)
#
# here we just sort in descending order
# first will get only the first row (note: ot strictly required tbh)
mostPopular = connections.sort(func.col("connections").desc()).first()
#
# agains similar to pandas
# create a filter which is true only if id matches mostPopular[0])
# apply the filter (gets back only rows with true)
# run a select of the name and get the first one
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")


#
# now let's find the least populars
connections.show()
# note: leastConnections is of type Row
leastConnections = connections.sort(func.col("connections").asc()).select("connections").first()
print(f"Least number of connections is {leastConnections[0]}") 
ids_with_least_connections = connections.filter(func.col("connections")==leastConnections[0]).select("id")
print("Super heros (IDs) with zero connections")
ids_with_least_connections.show()
#
# here we run a join
# join(self, other, on=None, how=None)
#
# Note another way to run the join is
# df1.join(<df2>, <column_name>)
heros_with_least_connections = ids_with_least_connections.join(names, # second df
                                                             ids_with_least_connections.id == names.id # on
                                                             # default is inner join which is alright
                                                            ).select("name")

print(f"These heros have all {leastConnections[0]} co-appearances.")
heros_with_least_connections.show()

