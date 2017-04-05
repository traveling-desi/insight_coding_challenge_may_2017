
#199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
#exec(open('process_log.py').read())

import io
import pandas as pd
import re
from time import time
import os



#script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
script_dir = "./"
#script_dir = "../"


payLoad = {}
for i in ['host', 'res', 'bytes', 'time', 'code']:
	payLoad[i] = []

rel_path = "log_input"
logDir = os.path.join(script_dir, rel_path)
fileName = logDir + '/log.txt'
start = time()
with io.open(fileName, 'r', encoding='windows-1252', errors='ignore') as infile:
	for line in infile:
		#temp = (re.sub(r'(?is)\[|\]|\"', '', line)).split()
		temp = (re.sub(r'(?is)\[|\]', '', line)).split()
		payLoad['host'].append(temp[0])
		payLoad['time'].append(temp[3])
		payLoad['res'].append(temp[-5] + " " + temp[-4] + " " + temp[-3])
		payLoad['code'].append(temp[-2])
		payLoad['bytes'].append(temp[-1])
end = time()
# Print the results
print("Read the file in {:.4f} seconds".format(end - start))


payLoad = pd.DataFrame(payLoad)
payLoad['bytes'] = pd.to_numeric(payLoad['bytes'], errors='coerce')
payLoad.index = pd.to_datetime(payLoad['time'], format="%d/%b/%Y:%H:%M:%S")
payLoad = payLoad.drop('time', axis = 1)
payLoad['nCounts'] = 1 	

rel_path = "log_output"
logDir = os.path.join(script_dir, rel_path)


## FEATURE1: Top hosts
print("Computing Feature 1")
start = time()
fileName = logDir + '/hosts.txt'
with open(fileName, 'a') as out_file:
	print(payLoad.reset_index()['host'].value_counts(sort=True, ascending=False)[0:9], file=out_file)
	#print(payLoad[['res','bytes']].groupby('res').count().sort_values(by='bytes',ascending=False)[0:9])
	#print(payLoad[['res','bytes']].groupby('res')['bytes'].sum().sort_values(ascending=False))

end = time()
# Print the results
print("Computed Feature 1 in  {:.4f} seconds".format(end - start))



## FEATURE2: Top resources
print("Computing Feature 2")
start = time()
fileName = logDir + '/resources.txt'
with open(fileName, 'a') as out_file:
	print(payLoad.reset_index().groupby('res')['bytes'].sum().sort_values(ascending=False)[0:9], file=out_file)
end = time()
# Print the results
print("Computed Feature 2 in  {:.4f} seconds".format(end - start))


## FEATURE 3: rolling 60 min time slots with most accesses
print("Computing Feature 3")
start = time()
fileName = logDir + '/hours.txt'
with open(fileName, 'a') as out_file:
	print(payLoad.rolling(window='h')['nCounts'].count().sort_values(ascending=False)[0:9], file=out_file)
	#print(payLoad.rolling(window='h')['bytes'].count().sort_values(ascending=False)[0:9])
end = time()
# Print the results
print("Computed Feature 3 in  {:.4f} seconds".format(end - start))




## FEATURE 4: finding events in 20s and 5m windows
print("Computing Feature 4")
start = time()
fileName = logDir + '/blocked.txt'
a = payLoad[payLoad.code == '401'].rolling(window='20s').count()
b = a[a.nCounts >= 3].reset_index()
b = b.join(b['time'] + pd.to_timedelta(1, unit='s'), rsuffix='_one')
b = b.join(b['time'] + pd.to_timedelta(300, unit='s'), rsuffix='_threehundred')
b = b.drop('time', axis=1)
payLoad = payLoad.drop('nCounts', axis=1)

def emit(record):
	record = list(record)
	host = record[2]
	t1 = record[5]
	t2 = record[6]
	print(payLoad[(payLoad.host == host) & (payLoad.index >= t1) & (payLoad.index <= t2)], file=out_file)

with open(fileName, 'a') as out_file:
	b.apply(emit, axis=1)

end = time()
# Print the results
print("Computed Feature 4 in  {:.4f} seconds".format(end - start))


'''

## 60 min time slots @ the top of the hour.
#print(payLoad.resample('H', label='left')['bytes'].sum().sort_values(ascending=False)[0:9])

## rolling 60 min time slots with most bandwidth
#print(payLoad.rolling(window='h')['bytes'].sum().groupby(by=payLoad.index).max().sort_values(ascending=False)[0:9])

a = payLoad[payLoad.code == '401'].rolling(window='20s').count()
b = a[a.nCounts >= 3].reset_index()
b = b.join(b['time'] + pd.to_timedelta(1, unit='s'), rsuffix='_one')
b = b.join(b['time'] + pd.to_timedelta(300, unit='s'), rsuffix='_threehundred')
b = b.drop('time', axis=1)

with open(fileName, 'a') as out_file:
	for t1, t2, host in b[['time_one', 'time_threehundred', 'host']].values.tolist():
     		print(payLoad[(payLoad.host == host) & (payLoad.index >= t1) & (payLoad.index <= t2)], file=out_file)



## 
## intList = []
## writeOutList = []
## for index in a[a.nCounts >= 3].index:
##      sum = 0
##      for i in payLoad.ix[index - pd.to_timedelta(20, unit='s'):index].reset_index().values.tolist():
##          intList.append(i)  
##          sum += 1
##          print (sum, i)
##      sum = 0
##      for i in payLoad.ix[index: index + pd.to_timedelta(300, unit='s')].reset_index().values.tolist():
##          writeOutList.append(i)  
##          sum += 1
##          print (sum, i)
## 

'''
