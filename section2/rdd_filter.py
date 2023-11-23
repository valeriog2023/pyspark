from pyspark import SparkConf, SparkContext

#
# run with spark-submit section2/rdd_filter.py

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext.getOrCreate(conf = conf)

#
# each line is i the format
# <station>,<date>,<type>,<value in tenth of degrees C>,....
# ITE00100554,18000101,TMAX,-75,,,E,
# ITE00100554,18000101,TMIN,-148,,,E,
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]  # first value
    entryType = fields[2]  # TMAX or TMIN
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0  # converts in farenight...
    return (stationID, entryType, temperature)

# lines is an rdd: resilient distributed dataset (RDD)
lines = sc.textFile("dataset/1800.csv")
parsedLines = lines.map(parseLine)
#
# filter takes as input a function that returns a boolean
# so here we get only the entries with min TMIN
# you can use this appraoch to filter logs for instance
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
#
# here we create a key value pair where the station 
# is the key and the temp value is the value
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
#
# here we aggregate and compare the keys and get the 
# for each key (staion) the min temperature recorded
# note you always need 2 values in the lambda to compare 2 entries 
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();                       # -> action

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))