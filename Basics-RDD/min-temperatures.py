'''
Input: WeatherStationID, DateYYYYMMDD, ObservationType, Value (Tenth of a Degree Celcius -75 -> -7.5C),,,
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,

Output: Minimum Temperature in a Year For Each Weather Station
'''
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemp")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("C:/Users/vijay/Documents/SparkCourse/Datasets/1800.csv")
parsedLines = lines.map(parseLine)

# Removes any non TMIN records
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# We don't need the ObservationType column anymore since its all TMIN, so remove it
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# Get the lowest temp for each station
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect()

for res in results:
    print(res[0] + "\t{:.2f}F".format(res[1]))