from pyspark.sql import SparkSession
from pyspark.sql import Row

def mapper(line):
    fields = line.split(',')
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode('utf=8')),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )

# Create a SparkSession
spark = SparkSession.builder.appName('SparkSql').getOrCreate()

# Read file and create RDD
lines = spark.sparkContext.textFile('C:/Users/vijay/Documents/SparkCourse/Datasets/fakefriends.csv')
# Turn each line of the RDD into a Row object since a DataFrame is a dataset of Row objects
people = lines.map(mapper) 

# Infer the schema and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 and age <= 19')

# The results of the SQL queries are RDDs and support all the normal operations
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy('age').count().orderBy('age').show()

spark.stop()
