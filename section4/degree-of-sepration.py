from typing import Tuple,List
#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)
sc.setLogLevel("WARN")

# We want to change our unstructured data into structured data
# the grapsh data file is in the format
# 5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456
# 5989 4080 4264 4446 3779 2430 2297 6169 3530 3272 4282 6432 2548 4140 185 105 3878 2429 1334 4595 2767 3956 3877 4776 4946 3407 128 269 5775 5121 481 5516 4758 4053 1044 1602 3889 1535 6038 533 3986
# 5982 217 595 1194 3308 2940 1815 794 1503 5197 859 5096 6039 2664 651 2244 528 284 1449 1097 1172 1092 108 3405 5204 387 4607 4545 3705 4930 1805 4712 4404 247 4754 4427 1845 536 5795 5978 533 3984 6056
# 5983 1165 3836 4361 1282 ...
#
# there the number of elements is variable, so we can change it as follows
# <id>, (list of connected heros), <distance>, <state>
# where
# id is the first element of the line
# list of connected heros is th remaning of the lines, split
# distance starts as infinite (we assume 9999)
# state referes to the Breadth first Search algorithm (check md file)
#       we start with white and allow gray and black as states
#
#
# Note as this is gong to run on multiple hosts  (potentially)
# we need a way to communicate when different hosts find a result
# this is done via accumulators
# which are vars shared between the cluster
#
# hitCounter = sc.accumulator(0)
#

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)
#
# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)


def convertToBFS(line:str):
    """
    This function converts a line from the file Marvel-graph.txt
    in the data required for the Breadth First Search algorithm
    (see notes in comments)
    """
    fields = line.split()
    heroID = int(fields[0])
    connections = [int(x) for x in fields[1:] ]

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
    #
    # note: this is an RDD so need a key and the rest packed together
    return (heroID, (connections, distance, color))


def createStartingRdd():
    """
    This returns a pyspark RDD
    First read the file: dataset/marvel-graph.txt
    second use map and function: convertToBFS
    to return an RDD
    The format will be
    (heroID, (connections, distance, color))
    color will be white for all rows except the starting hero
    """
    inputFile = sc.textFile("dataset/Marvel-graph.txt")
    return inputFile.map(convertToBFS)



def bfsMap(node):
    """
    This function expcts as input a list of values pyspark Row
    if the color is:
     - white: does nothing -> returns the single row unchanged
     - black: does nothing -> returns the single row unchanged
     - gray: creates
          - one new row for each connection and set it to gray
            Note: I'm pretty sure this can create loop since it
                  does not check if the new connection row has
                  been parsed already or not
                  Note: a follow up function removes the duplications
                        before the new iteration starts
          - one row for itself with same values but black color
          - if the targetCharacterID is a connection it adds 1
            to the hitCounter to
    """
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                #
                # need to use add as this is an accumulator
                hitCounter.add(1)

            # note that we creae an entry with no connection here
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results


def bfsReduce(data1, data2):
    """
    this gest as input the data from 2 rows with the same key Id
    """
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    # new nodes are created with no connections ([])
    # so here we just get the original conenctions
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # set minimum distance (between the 2)
    if distance1 < distance:
        distance = distance1
    else:
        distance = distance2

    # Preserve darkest color
    # if color1 is BLACK set BLACK
    if color1 == 'BLACK':
        color = 'BLACK'
    # if color1 is GRAY and color2 is BLACK, set BLACK
    elif color1 == 'GRAY' and color2 == 'BLACK':
        color = 'BLACK'
    # if color1 is GRAY and color2 is not BLACK, set GRAY
    elif color1 == 'GRAY':
        color = 'GRAY'
    # if color1 is not BLACK or GRAY, then set whatever color is color2
    else:
        color = color2

    return (edges, distance, color)


#Main program here:
if __name__ == "__main__":
    iterationRdd = createStartingRdd()

    for iteration in range(0, 10):
        print("Running BFS iteration# " + str(iteration+1))

        # Create new vertices as needed to darken or reduce distances in the
        # reduce stage. If we encounter the node we're looking for as a GRAY
        # node, increment our accumulator to signal that we're done.
        # here we take the initial RDD and because for each row we can
        # create multiple ones
        # Note that:
        #   -  the original rows stay where they are
        #   -  the gray rows are replaced by
        #       - new gray rows (new nodes)
        #       - one black row (replaces the original row)
        mapped = iterationRdd.flatMap(bfsMap)

        # Note that mapped.count() action here forces the RDD to be evaluated, and
        # that's the only reason our accumulator is actually updated.
        print("Processing " + str(mapped.count()) + " values.")

        #
        # hitcounter is shared so any host could have set it
        # if it is set, we stop..
        # possibly not great.. as that could be not the shortest distance..
        if (hitCounter.value > 0):
            print("Hit the target character! From " + str(hitCounter.value) \
                + " different direction(s).")
            break

        # Reducer combines data for each character ID,
        # basically because I had new rows created
        # we do have duplicates and we can remove them by:
        #  - preserving the darkest color
        #  - keeping shortest path.
        # so reduce will get as input the values from 2 rows with the same id
        iterationRdd = mapped.reduceByKey(bfsReduce)

