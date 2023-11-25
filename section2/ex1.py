from pyspark import SparkConf, SparkContext
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
# we want tog et the amount spent by each customer (i.e. the sum of the costs)
conf = SparkConf().setMaster("local").setAppName("section2_ex1")
sc = SparkContext(conf = conf)

input = sc.textFile("dataset/customer-orders.csv") # input is an pyspark RDD (basically a list of lines from the text file)
#
# from the line create an rdd with entries the customer code (int) and spend (changed to float)
# then reduce each keys by summing the values
customer_spend = input.map(lambda x: (int(x.split(',')[0]),float(x.split(',')[2])) ).reduceByKey(lambda x,y : x + y)
#
# get the result which is a list (customer_id, total_spend)
results=customer_spend.collect()
#
# sort byt total_spend and print
results.sort(key= lambda x: x[1])
for k,v in results:
    #
    # if I have a readable word I print it out with the count
    print(f"Customer: {k} :  spend({v})")
print("Now sorted by customer")    
results.sort()
for k,v in results:
    #
    # if I have a readable word I print it out with the count
    print(f"Customer: {k} :  spend({v})")    