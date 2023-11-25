# similar to section 2 word_count by here we want to use dataframes
# and "SQL"ish structure..
# this is more difficult because  the way we were doing is split a 
# text file in lines  and then looking for words.. each line however
# is different so the data is unstructured (so RDD is better in this case)
# 
# we are going to use the function func.explode()
# that similar to flatmap takes one input and returns multilpe outputs
# (explode columns into rows?)
#
# and other functions func.split(), func.lower(), etc...
#
# Note:you need to pass columns as parameter and there's a few ways
#  filter(wordsDF.word != "")
#  func.split(inputDF.value, "\\W+)
#  func.col("columnName")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
#


spark = SparkSession.builder.appName("SparkSQL_wordCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

inputDF = spark.read.text("dataset/Book.txt")
# at this point inputDF is a DF with a single column
# the column name is called: value (there's no schema and no header..)
# the value for each entry is the line of text from the file

#
# here we get the values from inputDF, then 
# for each entry, which is a line of text (start from the inner function):
#   - split it looking for words via a aregex
#   - call the function explode so that each entry in the list geenrate by split becomes a new row
#   - the resulting column is renamed as words
wordsDF = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("words"))
wordsDF.show()

#
# now we clean it up (make all lower) and group
# this is simlar to pandas, i.e. wordsDF.word != "" returns a boolean df
# and then you use to filter (get back only where we had true values)
wordsDF.filter(wordsDF.words != "")
wordsDF = wordsDF.select(func.lower(wordsDF.words).alias("words"))

#
# now we sort the result
# note the first line sort in ascending order
# to get the most used we need to apply desc to the col.. abit complicated if you ask me
wordsDF.groupBy("words").count().sort("count").show()
wordsDF.groupBy("words").count().sort(func.col("count").desc()).show()
