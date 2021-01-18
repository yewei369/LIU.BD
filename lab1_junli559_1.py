## Jun Li, junli559
## Lab 1, Big Data Analysis, 732A74

from pyspark import SparkContext

### Task 1 ###
sc = SparkContext(appName = "task 1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")  # This path is to the file on hdfs
lines = temperature_file.map(lambda line: line.split(";")) # get info from lines
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))   # (key, value) = (year,temperature)
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)  #filter valid years

max_temperatures = year_temperature.reduceByKey(lambda a,b:a if a>b else b)  #Get max
min_temperatures = year_temperature.reduceByKey(lambda a,b:b if a>b else a)  #Get min
maxmin_temperatures=max_temperatures.union(min_temperatures) #merge RDD
maxmin_temperatures=maxmin_temperatures.reduceByKey(lambda a,b:(a,b) if a>b else (b,a)) #get max/min pairs
maxmin_temperatures = maxmin_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0]) #sort by max
#print(max_temperatures.collect())
maxmin_temperatures.saveAsTextFile("BDA/output/task1")  #save the result into /user/ACCOUNT_NAME/BDA/output


### Task 2 ###
sc = SparkContext(appName = "task 2")


temperature_file = sc.textFile("BDA/input/temperature-readings.csv")  # This path is to the file on hdfs
lines = temperature_file.map(lambda line: line.split(";")) # get info from lines

month_temperature1 = lines.map(lambda x: ((x[1][0:4],x[1][5:7]),float(x[3])))   # (key, value) = ((year,month),temperature)
month_temperature1 = month_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)  #filter valid years
num1_temperatures=month_temperature1.mapValues(lambda x:1).reduceByKey(lambda a,b:a+b).sortByKey(ascending = False)  # part 1 
num1_temperatures.saveAsTextFile("BDA/output/task2.1")  #save the result into /user/ACCOUNT_NAME/BDA/output

month_temperature2=lines.map(lambda x: ((x[1][0:4],x[1][5:7]),float(x[3])))   # (key, value) = ((year,month),temperature)
month_temperature2 = month_temperature2.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>10)  #filter valid years
num2_temperatures=month_temperature2.distinct()  # part 2, retain only distinct values
num2_temperatures=num2_temperatures.mapValues(lambda x:1).reduceByKey(lambda a,b:a+b).sortBy(ascending = False, keyfunc=lambda k: k[1]) # part 2
num2_temperatures.saveAsTextFile("BDA/output/task2.2") 


### Task 3 ###
sc = SparkContext(appName = "task 3")


temperature_file = sc.textFile("BDA/input/temperature-readings.csv")  # This path is to the file on hdfs
lines = temperature_file.map(lambda line: line.split(";")) # get info from lines

day_temperature=lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:10],x[0]),float(x[3])))   # (key, value) = ((year,month,day,station),temperature)
day_temperature= day_temperature.filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014)  #filter valid years

max_temperatures = day_temperature.reduceByKey(lambda a,b:a if a>b else b)  #Get daily max
min_temperatures = day_temperature.reduceByKey(lambda a,b:b if a>b else a)  #Get daily min
maxmin_temperatures=max_temperatures.union(min_temperatures) #merge RDD

maxmin_temperatures=maxmin_temperatures.map(lambda x: ((x[0][0],x[0][1],x[0][3]),(x[1],1))) # remap to ((year,month,station),(temperature,1))
maxmin_temperatures=maxmin_temperatures.reduceByKey(lambda a,b:a+b) #get sum and number of max/min
avg_temperatures=maxmin_temperatures.mapValues(lambda x:x[0]/x[1]).sortByKey(ascending = False) # get average
avg_temperatures.saveAsTextFile("BDA/output/task3") 



### Task 4 ###
sc = SparkContext(appName = "task 4")


temperature_file = sc.textFile("BDA/input/temperature-readings.csv")  # This path is to the file on hdfs
temperature_lines = temperature_file.map(lambda line: line.split(";")) # get info from lines
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")  # This path is to the file on hdfs
precipitation_lines = precipitation_file.map(lambda line: line.split(";")) # get info from lines

station_temperature = temperature_lines.map(lambda x: (x[0], float(x[3])))   # (key, value) = (station,temperature)
max_temperatures = station_temperature.reduceByKey(lambda a,b:a if a>b else b)  #Get max temperature
max_temperatures=max_temperatures.mapValues(lambda x: (x,0))  

station_precipitation = precipitation_lines.map(lambda x: ((x[1][0:4],x[1][5:7],x[1][8:10],x[0]),float(x[3])))   # (key, value) = ((year,month,day,station),precipitation)
max_precipitations=station_precipitation.reduceByKey(lambda a,b:a+b)  #Get daily precipitation
max_precipitations=max_precipitations.map(lambda x:(x[0][3],x[1]))    #remap to (station,daily precipitation)
max_precipitations = max_precipitations.reduceByKey(lambda a,b:a if a>b else b)  #Get max precipitation
max_precipitations=max_precipitations.mapValues(lambda x: (0,x))  
 
max_combine=max_temperatures.union(max_precipitations).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))  # combine temperatures and precipitations
max_combine=max_combine.filter(lambda x: float(x[1][0])>=25.0 and float(x[1][0])<=30.0 and  x[1][1]>=100.0 and x[1][1]<=200.0)  #filter valid temperatures&precipitations
max_combine.saveAsTextFile("BDA/output/task4")



### Task 5 ###
sc = SparkContext(appName = "task 5")

station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")  # This path is to the file on hdfs
station_lines=station_file.map(lambda line: line.split(";")) # get info from lines
stations=station_lines.map(lambda x:x[0])  # get station ID
stations=sc.broadcast(stations.collect()) # make broadcast variable

data_file=sc.textFile("BDA/input/precipitation-readings.csv") # This path is to the file on hdfs
data_lines=data_file.map(lambda line: line.split(";")) # get info from lines
data=data_lines.map(lambda x:((x[1][0:4],x[1][5:7],x[0]),float(x[3]))) # (key, value) = ((year,month,station),precipitation)
data=data.filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014 and (x[0][2] in stations.value))  # filter valid readings 

data=data.reduceByKey(lambda a,b: a+b)  # get monthly precipitation for each station 
data=data.map(lambda x: ((x[0][0],x[0][1]),(x[1],1))).reduceByKey(lambda a,b:a+b).mapValues(lambda x:x[0]/x[1]).sortByKey(ascending = False)


data.saveAsTextFile("BDA/output/task5")
