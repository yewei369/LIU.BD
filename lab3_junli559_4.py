## Jun Li, junli559
## Lab 3, Big Data Analysis, 732A74


from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
import numpy as np
#import matplotlib.pyplot as plt

sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
  """
	Calculate the great circle distance between two points
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians
  lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula
  dlon = lon2 - lon1
  dlat = lat2 - lat1
  a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
  c = 2 * asin(sqrt(a))
  km = 6367 * c
  return km

	
h_distance = 500# Up to you
h_date = 5# Up to you
h_time = 5# Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-11-04" # Up to you
stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

# Your code here

# 1.prepare and broardcast stations
linesSta=stations.map(lambda line: line.split(";")) # get info from lines
stationsRDD=linesSta.map(lambda x:(x[0],(float(x[4]),float(x[3])))) # map to (station,longitude,latitude)
linesTemp=temps.map(lambda line: line.split(";")) # get info from lines
tempsRDD=linesTemp.map(lambda x: (x[0],x[1],x[2],float(x[3]))).filter(lambda x:x[1]<=date).cache()   # map to (station,date,time,temperature) and filter away later dates
#tempsRDD=tempsRDD.sample(False,0.1).cache()
num=tempsRDD.count() # number of readings
#stationsRDD.cache()  # broadcast RDD
stationsRDD=stationsRDD.collectAsMap()
bc=sc.broadcast(stationsRDD)
print('HAHA:\n')
#station_all=tempsRDD.map(lambda x:x[0]).collect()   # get training data of stations
#date_all=tempsRDD.map(lambda x:x[1]).collect()   # get training data of dates
#time_all=tempsRDD.map(lambda x:x[2]).collect()   # get training data of times
data=tempsRDD.map(lambda x:x[3]).collect()   # get training data of temperatures
print('Step1 done!')




# 2.compute kernels 
def kernelTemp(b,a,date,time,h_distance,h_date,h_time):
  
  """"
	b/a, longitude/latitude;
	date/time, date/time to predict;
	h_distance/h_date/h_time, width parameter;
	"""

  date=datetime.strptime(date,'%Y-%m-%d')
  rad_date=2*np.pi*((date-datetime.strptime((str(date.year)+'-01-01'),'%Y-%m-%d')).days/365)
  sin_date=sin(rad_date)
  cos_date=cos(rad_date)
  
  time=datetime.strptime(time,'%H:%M:%S')
  rad_time=2*np.pi*((time-datetime.strptime('00:00:00','%H:%M:%S')).seconds/(24*3600))
  sin_time=sin(rad_time)
  cos_time=cos(rad_time)
	
  def kernel(x):
    x_station=x[0]
    x_date=x[1]
    x_time=x[2]
 
    x_coordinate=bc.value[x_station] # get coordinates of valid station
    x_b=x_coordinate[0]
    x_a=x_coordinate[1]
    k_distance=haversine(b, a, x_b, x_a) # get distance in geography
    k_distance=exp(-(k_distance/h_distance)**2) # get kernel in geography
	    
    x_date=datetime.strptime(x_date,'%Y-%m-%d')
    rad_x_date=2*np.pi*((x_date-datetime.strptime((str(x_date.year)+'-01-01'),'%Y-%m-%d')).days/365)
    sin_x_date=sin(rad_date)
    cos_x_date=cos(rad_date)
    k_date=sqrt((sin_date-sin_x_date)**2+(cos_date-cos_x_date)**2) # get distance in date
    k_date=exp(-(k_date/h_date)**2) # get kernel in date
    
    x_time=datetime.strptime(x_time,'%H:%M:%S')
    rad_x_time=2*np.pi*((x_time-datetime.strptime('00:00:00','%H:%M:%S')).seconds/(24*3600))
    sin_x_time=sin(rad_x_time)
    cos_x_time=cos(rad_x_time)
    k_time=sqrt((sin_time-sin_x_time)**2+(cos_time-cos_x_time)**2) # get distance in time
    k_time=exp(-(k_time/h_time)**2)  # get kernel in time
    
    return (k_distance,k_date,k_time,x[3]) ## (k_distance,k_date,k_time,temperature)
     
  return tempsRDD.map(kernel)
 

x=["23:59:59", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]
predict1=np.zeros(11)
predict2=np.zeros(11)

for i,time in enumerate(x):
  new=kernelTemp(b,a,date,time,h_distance,h_date,h_time).cache() # get RDD with kernels for time
  
  # 3.Method 1: sum of kernels  
  k1=new.map(lambda x:(x[0]+x[1]+x[2],(x[0]+x[1]+x[2])*x[3])).reduce(lambda a,b:(a[0]+b[0],a[1]+b[1]))
  predict1[i]=k1[1]/k1[0]

  # 4.Method 2: product of kernels 
  k2=new.map(lambda x:(x[0]*x[1]*x[2],(x[0]*x[1]*x[2])*x[3])).reduce(lambda a,b:(a[0]+b[0],a[1]+b[1]))
  predict2[i]=k2[1]/k2[0]

print('Step2+3+4 done!\n')


# 5.save plot
print("\nPredictions from method 1: ")
print(predict1)
print("Predictions from method 2: ")
print(predict2)

print('\nAll done!\n')

# Your code here
