from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import time
#
#
# in this exercise we expect the data to be in the csv format
# as 
# 47,6694,14.98
# 29,680,13.08
# 91,8900,24.59
# 70,3959,68.68
# where the first value is the customer id
# the second value is the product id
# the third value is cost
#
#
# we use this to create an explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Min_expanse_df").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# the schema needs to be struct type
# made of structFields
# note the last True is the nullable flag, i.e. cna the field be null
schema = StructType([ 
                     StructField("customerId", IntegerType(), False), 
                     StructField("productId", IntegerType(), False), 
                     StructField("cost", FloatType(), False)
                     ])

# // Read the file passing the schema: schema
df = spark.read.schema(schema).csv("dataset/customer-orders.csv")
df.printSchema()

# Group by customer ID
customers_total_costs = df.groupBy("customerId").sum()
new_df = customers_total_costs["customerId", 
                               func.round(func.col("sum(cost)"),2).alias("totals")] \
                                  .sort(func.col("totals").desc())
new_df.show()

time.sleep(8)
#
# alternative solution
customers_total_costs = df.groupBy("customerId").agg(func.round(func.sum("cost"), 2) \
                                                     .alias("total spent"))
sorted_customers_total_costs = customers_total_costs.sort(func.col("total spent").desc())
sorted_customers_total_costs.show(sorted_customers_total_costs.count())

spark.stop()

                                                  