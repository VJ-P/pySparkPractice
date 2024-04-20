# Import the Spark Context and Configuration Libraries
from pyspark import SparkConf, SparkContext
import collections

# Initialize the conf and then the sc
# This is letting the context know that we are running a single threaded on the local machine
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Lines is an RDD which contains a string for each line in the data file
# This is why the data is then split by whitespace
# The 3rd value (2nd iterator) contains the rating which is what we want to group by
lines = sc.textFile("C:/Users/vijay/Documents/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()


sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
