from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("dataset/Book.txt") # input is an pyspark RDD (basically a list of lines from the text file)

# words is now a pipelinedRDD, which is a subclass of an RDD, the code is sent to the worker
# and executed (in this case localhost) and flatmap returns multiple entries for every initial entry (splitting in this case)
# Remember that map instead create a single entry out for every entry in
words = input.flatMap(lambda x: x.split())

# countbyValue returns a defaultDict (value,count pairs) in this case value is words
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
#
#
############################### Now we improve it a little
#
# a little better in terms of findin actual words
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())
#
# here we send an actual function instead of the lambda
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    #
    # this will encode word in ascii (the encoder)..  errors may be given to set a different error handling scheme
    # Default is 'strict' meaning that encoding errors raise a UnicodeEncodeError.
    # Other possible values are 'ignore', 'replace' and 'xmlcharrefreplace' as well as any other name registered with
    # codecs.register_error that is able to handle UnicodeEncodeErrors.
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        #
        # word is now encoded in ascii so we can't concatenate a string directly.. need to decode() first
        print(cleanWord.decode() + " " + str(count))


#
#
############################### Now we want to implement sorting
#
# so we need to implement a manul count by values
# we use:
#     map: single in value to single out value and we map every word to the tuple with itself the value 1
#     reduceByKey: at every value generated we apply reduce by key with a lmbbda to sum values,
#                  i.e. we keep the key (word) and we sum the values (the 1 we just added)
# the result is stored in a new rdd(pipelined)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)
#
# now we want to sort based on the number (second value in the tuple, so we flip the 2 values) and sort by keys
# note python lambda in this case does not unpack the tuple x (x here is (word, count) ) so I get an error
# if I use wordCounts.map(lambda x,y: y, x).sortByKey() as suggested
# Note also that I only get the error when I run the collect statement
#sortedWordCounts = wordCounts.map(lambda x: (x[1],x[0])).sortByKey()
#
# other note:
# don't really need to flip them around.. I can jsut count based on the second value like this:
#
# sortedWordCounts = wordCounts.sortBy(lambda x: x[1])
# (if you uncomment this, you need to fli the order in the loop after) 
#
# now we collect the results (this is the action that triggers the execution) and returns a list
results = sortedWordCounts.collect()
print(type(results))
for result in results:
    #
    # if I have a readable word I print it out with the count
    if result[1].encode('ascii','ignore'):
        print(f"{result[1]:20}  : {result[0]}")
