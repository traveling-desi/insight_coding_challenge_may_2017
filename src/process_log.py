
#199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
#exec(open('process_log.py').read())

import io
import pandas as pd
import re
from time import time


script_dir = "./"
#script_dir = "../"


############
##### Function to create DatFrame from a list and return it
###########
def createDF(payLoadSet, fields):
	payLoad = {}
	for (i, j) in fields:
		payLoad[i] = []
	for i in ['record', 'timeStr']:
		payLoad[i] = []

	for line in payLoadSet:
		#temp = (re.sub(r'(?is)\[|\]|\"', '', line)).split()
		temp = (re.sub(r'(?is)\[|\]', '', line)).split()
		timeStr = re.search(r'\[(.*)\]', line)
		for (i, j) in fields:
			payLoad[i].append(temp[j])
		payLoad['record'].append(str(line))
		payLoad['timeStr'].append(timeStr.group(1))
	payLoad = pd.DataFrame(payLoad)
	return payLoad



rel_path = "log_input"
logDir = script_dir + rel_path
fileName = logDir + '/log.txt'
start = time()
with io.open(fileName, 'r', encoding='windows-1252', errors='ignore') as infile:
	payLoadSet = []
	for line in infile:
		payLoadSet.append(line)

	payLoad = createDF(payLoadSet, [('host', 0), ('time', 3), ('res', -4), ('code', -2), ('bytes', -1)])
	payLoad['bytes'] = pd.to_numeric(payLoad['bytes'], errors='coerce')
	payLoad.index = pd.to_datetime(payLoad['time'], format="%d/%b/%Y:%H:%M:%S")
	payLoad = payLoad.drop('time', axis = 1)
	payLoad['nCounts'] = 1 	

end = time()
# Print the results
print("Read the file in {:.4f} seconds".format(end - start))


rel_path = "log_output"
logDir = script_dir + rel_path



############
## FEATURE1: Top hosts
##############
print("Computing Feature 1")
start = time()
fileName = logDir + '/hosts.txt'
with open(fileName, 'w') as out_file:
	print(payLoad.reset_index()['host'].value_counts(sort=True, ascending=False)[0:9].to_csv(), file=out_file)

end = time()
# Print the results
print("Computed Feature 1 in  {:.4f} seconds".format(end - start))



############
## FEATURE2: Top resources
############
print("Computing Feature 2")
start = time()
fileName = logDir + '/resources.txt'
with open(fileName, 'w') as out_file:
	print(payLoad.reset_index().groupby('res')['bytes'].sum().sort_values(ascending=False).reset_index().loc[0:9, ['res']].to_csv(sep=' ', index=False, header = False), file=out_file)
end = time()
# Print the results
print("Computed Feature 2 in  {:.4f} seconds".format(end - start))





############
## FEATURE 3: rolling 60 min time slots with most accesses
############
print("Computing Feature 3")
start = time()
fileName = logDir + '/hours.txt'
with open(fileName, 'w') as out_file:
	print(payLoad.rolling(window='h')[['timeStr', 'nCounts']].count().sort_values(by='nCounts', ascending=False)[0:9].to_csv(sep=',', index=False, header = False), file=out_file)
end = time()
# Print the results
print("Computed Feature 3 in  {:.4f} seconds".format(end - start))




############
## FEATURE 4: finding events in 20s and 5m windows
############
print("Computing Feature 4")
start = time()
fileName = logDir + '/blocked.txt'
a = payLoad[payLoad.code == '401'].rolling(window='20s').count()
b = a[a.nCounts >= 3].reset_index()
b = b.join(b['time'] + pd.to_timedelta(1, unit='s'), rsuffix='_one')
b = b.join(b['time'] + pd.to_timedelta(300, unit='s'), rsuffix='_threehundred')
b = b.drop('time', axis=1)
payLoad = payLoad.drop('nCounts', axis=1)

payLoadSet = []

def emit(record):
	record = list(record)
	host = record[2]
	t1 = record[7]
	t2 = record[8]
	payLoadSet.append(payLoad[(payLoad.host == host) & (payLoad.index >= t1) & (payLoad.index <= t2)].reset_index()['record'])

with open(fileName, 'w') as out_file:
	b.apply(emit, axis=1)
	payLoadSet = set([item for sublist in payLoadSet for item in sublist])
	payLoad = createDF(payLoadSet, [('time', 3)])
	payLoad.drop_duplicates()
	payLoad.index = pd.to_datetime(payLoad['time'], format="%d/%b/%Y:%H:%M:%S")
	payLoad = payLoad.sort_index()
	payLoad = payLoad.drop(['time', 'timeStr'], axis=1)
	print("".join(payLoad.reset_index()['record'].values) , file=out_file)

end = time()
# Print the results
print("Computed Feature 4 in  {:.4f} seconds".format(end - start))





############
## End of Code


'''
The commands from here on down are not run. They have been commented out. There are additional feature implemented here.

## 60 min time slots @ the top of the hour.
print(payLoad.resample('H', label='left')['bytes'].sum().sort_values(ascending=False)[0:9])

## rolling 60 min time slots with most bandwidth
print(payLoad.rolling(window='h')['bytes'].sum().groupby(by=payLoad.index).max().sort_values(ascending=False)[0:9])

'''
