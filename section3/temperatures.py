from pyspark.sql import SparkSession
from pyspark.sql import functions as func
#
# w use this to create an explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# the schema needs to be struct type
# made of structFields
# note the last True is the nullable flag, i.e. cna the field be null
schema = StructType([ 
                     StructField("stationID", StringType(), True), 
                     StructField("date", IntegerType(), True), 
                     StructField("measure_type", StringType(), True), 
                     StructField("temperature", FloatType(), True)
                     ])

# // Read the file passing the schema: schema
df = spark.read.schema(schema).csv("dataset/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
# measure_type is the name of the column
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()  # -> note: show is an action, so nothing happen until here
#                                    overall in prod you might want to remove them
#                                    because they break the execution in points
#                                    If you can have everything pipelined and distributed behind the
#                                    the scenes is more efficient
#
# Convert temperature to Celsius (they are tenth of) and sort the dataset
# Note: here we also create a new "temperature" column called: "Celsius Temperature"
minTempsByStationF = minTempsByStation.withColumn("Celsius Temperature",
                                                  func.round(func.col("min(temperature)") * 0.1, 2)) \
                                                    .select("stationID", "Celsius Temperature") \
                                                        .sort("Celsius Temperature")
                                                  
# Collect, format, and print the results
results = minTempsByStationF.collect()  # <- another action

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
    
spark.stop()

                                                  