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
