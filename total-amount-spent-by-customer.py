'''
Input: CustomerID, ItemID, AmountSpent
44,8602,37.19
35,5368,65.89
2,3391,40.64
47,6694,14.98
29,680,13.08

Output: Total Amount Spent by CustomerID
(44, 77.83)
(35, 78.97)
(47, 14.98)
'''
from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID, amountSpent)

conf = SparkConf().setMaster("local").setAppName("MaxTemp")
sc = SparkContext(conf=conf)

lines = sc.textFile("C:/Users/vijay/Documents/SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
amountSpent = parsedLines.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = amountSpent.collect()

for res in results:
    print(f"{res[1]} : {res[0]}")
