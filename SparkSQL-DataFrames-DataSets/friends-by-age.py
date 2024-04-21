'''
Input: fakefriends-header.py
userID,name,age,friends
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21

Output: Find the average number of friends by age: res=(age, average # of friends)
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

# This creates a dataframe with an inferred schema
people = spark.read.option('header', 'true').option('inferSchema', 'true')\
    .csv('C:/Users/vijay/Documents/SparkCourse/Datasets/fakefriends-header.csv')

# Compute the average
friendsByAge = people.select('age', 'friends')
print('Group by age')
friendsByAge.groupBy('age').avg('friends').sort('age').show()

# Format more nicely
friendsByAge.groupBy('age').agg(func.round(func.avg('friends'), 2)).alias('friends_avg').sort('age').show()

spark.stop()
