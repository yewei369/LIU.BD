## Jun Li, junli559
## Lab 2, Big Data Analysis, 732A74


from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

### Task 1 ###
sc =SparkContext(appName = "task 1")
sqlContext=SQLContext(sc)

rdd=sc.textFile("BDA/input/temperature-readings.csv") # This path is to the file on hdfs
parts= rdd.map(lambda line: line.split(";"))   # get info from lines

# Create a DataFrame from a RDD 1
#tempReadings=parts.map(lambda p: Row( station=p[0],year =p[1].split("-")[0],month=p[1].split("-")[1],date=p[1].split("-")[2],time=p[2],value =float(p[3]), quality =p[4]))
#schemaTempReadings=sqlContext.createDataFrame(tempReadings) #Inferring the schema and registering the DataFrame as a table
#schemaTempReadings.registerTempTable("tempReadings")

# Create a DataFrame from a RDD 2
tempReadingsRow= parts.map(lambda p: (p[0], int(p[1].split("-")[0]) ,int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
tempReadingsString= ["station","year","month","day","time","value","quality"]  #column names
schemaTempReadings= sqlContext.createDataFrame(tempReadingsRow,tempReadingsString) #create data frame
schemaTempReadings.registerTempTable("tempReadingsTable") #name of data frame

schemaTempReadingsMin=schemaTempReadings.filter("year>=1950 and year<=2014").groupBy('year','station').agg(F.min('value').alias('yearlymin')).orderBy (['yearlymin','year'],ascending=[0,0])  # API sql query for yearly min temperature
schemaTempReadingsMin.rdd.saveAsTextFile("BDA/output/task1.1")
schemaTempReadingsMax=schemaTempReadings.filter("year>=1950 and year<=2014").groupBy('year','station').agg(F.max('value').alias('yearlymax')).orderBy (['yearlymax','year'],ascending=[0,0])  # API sql query for yearly max temperature
schemaTempReadingsMax.rdd.saveAsTextFile("BDA/output/task1.2") 

#Use API methods, such as select(),filter(),groupBy(), agg(),alias() ,orderBy(), etc
#year, station with the max, maxValue ORDER BY maxValue DESC
#year, station with the min, minValue ORDER BY minValue DESC

### Task 2 ###

sc =SparkContext(appName = "task 2")
sqlContext=SQLContext(sc)

rdd=sc.textFile("BDA/input/temperature-readings.csv") # This path is to the file on hdfs
parts= rdd.map(lambda line: line.split(";"))   # get info from lines

tempReadingsRow= parts.map(lambda p: (p[0], int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
tempReadingsString= ["station","year","month","day","time","value","quality"] #column names
schemaTempReadings= sqlContext.createDataFrame(tempReadingsRow,tempReadingsString) #create data frame
schemaTempReadings.registerTempTable("tempReadingsTable") #name of data frame

schemaTempReadings=schemaTempReadings.filter("year>=1950 and year<=2014 and value>10") #filter valid conditions
schemaTempReadingsCount=schemaTempReadings.groupBy('year','month').count().orderBy (['count','year','month'],ascending=[0,0,0])  # API sql query for counts of valid temperatures
schemaTempReadingsCount.rdd.saveAsTextFile("BDA/output/task2.1")
schemaTempReadingsCountDistinct=schemaTempReadings.groupBy('year','month','value').count().groupBy('year','month').count().orderBy (['count','year','month'],ascending=[0,0,0])  # API sql query for counts of distinct valid temperatures
schemaTempReadingsCountDistinct.rdd.saveAsTextFile("BDA/output/task2.2")

#year, month, value ORDER BY value DESC
#year, month, value ORDER BY value DESC

### Task 3 ###

sc =SparkContext(appName = "task 3")
sqlContext=SQLContext(sc)

rdd=sc.textFile("BDA/input/temperature-readings.csv") # This path is to the file on hdfs
parts= rdd.map(lambda line: line.split(";"))   # get info from lines

tempReadingsRow= parts.map(lambda p: (p[0], int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
tempReadingsString= ["station","year","month","day","time","value","quality"] #column names
schemaTempReadings= sqlContext.createDataFrame(tempReadingsRow,tempReadingsString) #create data frame
schemaTempReadings.registerTempTable("tempReadingsTable") #name of data frame

schemaTempReadings=schemaTempReadings.filter("year>=1960 and year<=2014") #filter valid conditions
schemaTempReadingsAvg=schemaTempReadings.groupBy('year','month','station').agg(F.mean('value').alias('avgMonthlyTemperature')).orderBy ('avgMonthlyTemperature',ascending=0)  # API sql query for avgMonthlyTemperature
schemaTempReadingsAvg.rdd.saveAsTextFile("BDA/output/task3")

#year, month, station, avgMonthlyTemperature ORDER BY avgMonthlyTemperature DESC

### Task 4 ###

sc =SparkContext(appName = "task 4")
sqlContext=SQLContext(sc)

rddTemp=sc.textFile("BDA/input/temperature-readings.csv") # This path is to the file on hdfs
partsTemp= rddTemp.map(lambda line: line.split(";"))   # get info from lines
tempReadingsRow= partsTemp.map(lambda p: (p[0], int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
tempReadingsString= ["station","year","month","day","time","value","quality"] #column names
schemaTempReadings= sqlContext.createDataFrame(tempReadingsRow,tempReadingsString) #create data frame
schemaTempReadings.registerTempTable("tempReadingsTable") #name of data frame
schemaTempReadingsMax=schemaTempReadings.groupBy('station').agg(F.max('value').alias('maxTemperature'))  # get max temperature per station

rddPrec=sc.textFile("BDA/input/precipitation-readings.csv") # This path is to the file on hdfs
partsPrec= rddPrec.map(lambda line: line.split(";"))   # get info from lines
precReadingsRow= partsPrec.map(lambda p: (p[0], int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
precReadingsString= ["station","year","month","day","time","value","quality"] #column names
schemaPrecReadings= sqlContext.createDataFrame(precReadingsRow,precReadingsString) #create data frame
schemaPrecReadings.registerTempTable("precReadingsTable") #name of data frame
schemaPrecReadingsMax=schemaPrecReadings.groupBy('year','month','day','station').agg(F.sum('value').alias('DailyPrecipitation')).groupBy('station').agg(F.max('DailyPrecipitation').alias('maxDailyPrecipitation'))  # get max daily precipitation per station

final=schemaTempReadingsMax.join(schemaPrecReadingsMax,'station','outer')#merge data frame
final=final.filter("maxTemperature>=25 and maxTemperature<=30 and maxDailyPrecipitation>=100 and maxDailyPrecipitation<=200").orderBy('station',ascending=0) #filter valid conditions


final.rdd.saveAsTextFile("BDA/output/task4")

#station, maxTemp, maxDailyPrecipitation ORDER BY station DESC


### Task 5 ###

sc =SparkContext(appName = "task 5")
sqlContext=SQLContext(sc)

rddStation=sc.textFile("BDA/input/stations-Ostergotland.csv") # This path is to the file on hdfs
partsStation= rddStation.map(lambda line: line.split(";"))   # get info from lines
stations=partsStation.map(lambda x:x[0])  # get station ID
stations=sc.broadcast(stations.collect()) # make broadcast variable

rddPrec=sc.textFile("BDA/input/precipitation-readings.csv") # This path is to the file on hdfs
partsPrec= rddPrec.map(lambda line: line.split(";"))   # get info from lines
precReadingsRow= partsPrec.map(lambda p: (p[0], int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]), p[2], float(p[3]), p[4])) #map rows
precReadingsString= ["station","year","month","day","time","value","quality"] #column names
schemaPrecReadings= sqlContext.createDataFrame(precReadingsRow,precReadingsString) #create data frame
schemaPrecReadings.registerTempTable("precReadingsTable") #name of data frame
schemaPrecReadings=schemaPrecReadings.filter("year>=1960 and year<=2014") # filter valid conditions upon year
schemaPrecReadings=schemaPrecReadings.where(schemaPrecReadings['station'].isin(stations.value))# filter valid conditions upon station

schemaPrecReadingsMonth=schemaPrecReadings.groupBy('year','month','station').agg(F.sum('value').alias('MonthlyPrecipitation')) # get monthly precipitation
final=schemaPrecReadingsMonth.groupBy('year','month').agg(F.avg('MonthlyPrecipitation').alias('avgMonthlyPrecipitation')).orderBy(['year','month'],ascending=[0,0]) #get mean monthly regional precipitation


final.rdd.saveAsTextFile("BDA/output/task5")


#year, month, avgMonthlyPrecipitation ORDER BY year DESC, month DESC
