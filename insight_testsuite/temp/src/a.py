
#199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
#exec(open('a.py').read())

import io
import pandas as pd
import re
#with io.open("log.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
with io.open("temp.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
#with io.open("temp_1.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
#with io.open("temp_2.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
#with io.open("temp_3.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
#with io.open("temp_4.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
#with io.open("temp_5.txt", 'r', encoding='windows-1252', errors='ignore') as infile:
	payLoad = {}
	for i in ['host', 'res', 'bytes', 'time', 'code']:
		payLoad[i] = []


	for line in infile:
		temp = (re.sub(r'(?is)\[|\]|\"', '', line)).split()
		payLoad['host'].append(temp[0])
		payLoad['time'].append(temp[3])
		payLoad['res'].append(temp[-4])
		payLoad['code'].append(temp[-2])
		payLoad['bytes'].append(temp[-1])


ip_pd = pd.DataFrame(payLoad)
#ip_pd = pd.DataFrame({'host' : host, 'res' : res, 'bytes' : bytes, 'time' : time, 'code' : code})
ip_pd['bytes'] = pd.to_numeric(ip_pd['bytes'], errors='coerce')
ip_pd.index = pd.to_datetime(ip_pd['time'], format="%d/%b/%Y:%H:%M:%S")
ip_pd = ip_pd.drop('time', axis = 1)
ip_pd['dummy_numeric'] = 1 	

## Top hosts
print(ip_pd['host'].value_counts(sort=True, ascending=False)[0:9])
#print(ip_pd[['res','bytes']].groupby('res').count().sort_values(by='bytes',ascending=False)[0:9])
#print(ip_pd[['res','bytes']].groupby('res')['bytes'].sum().sort_values(ascending=False))

## Top resources
print(ip_pd.groupby('res')['bytes'].sum().sort_values(ascending=False)[0:9])

## 60 min time slots @ the top of the hour.
print(ip_pd.resample('H', label='left')['bytes'].sum().sort_values(ascending=False)[0:9])

## rolling 60 min time slots with most accesses
#print(ip_pd.rolling(window='h')['bytes'].count().sort_values(ascending=False)[0:9])
print(ip_pd.rolling(window='h')['dummy_numeric'].count().sort_values(ascending=False)[0:9])

## rolling 60 min time slots with most bandwidth
print(ip_pd.rolling(window='h')['bytes'].sum().groupby(by=ip_pd.index).max().sort_values(ascending=False)[0:9])


## ## finding events in 20s and 5m windows
a = ip_pd[ip_pd.code == '401'].rolling(window='20s').count()
b = a[a.dummy_numeric >= 3].reset_index()
for index, host in b[['time', 'host']].values.tolist():
     print(ip_pd[(ip_pd.host == host) & (ip_pd.index >= index + pd.to_timedelta(1, unit='s')) & (ip_pd.index <= index + pd.to_timedelta(300, unit='s'))])



## 
## intList = []
## writeOutList = []
## for index in a[a.dummy_numeric >= 3].index:
##      sum = 0
##      for i in ip_pd.ix[index - pd.to_timedelta(20, unit='s'):index].reset_index().values.tolist():
##          intList.append(i)  
##          sum += 1
##          print (sum, i)
##      sum = 0
##      for i in ip_pd.ix[index: index + pd.to_timedelta(300, unit='s')].reset_index().values.tolist():
##          writeOutList.append(i)  
##          sum += 1
##          print (sum, i)
## 
