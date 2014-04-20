import json
from pprint import pprint
import csv
import re

str = ""
d ={}
i=0
tweet = "I have Heck disease"

def read():
	json_data=open('input_2.json')
	data = json.load(json_data)
	loop(data)
	

def loop(data):
	global i
	global str
	global d
	for key in data:
		str=""
		for val in data[key]:
			i +=1
			str += val + "|"
		str = str[:-1]
		d[str]=key
	write(d)

		
		
def write(d):
	w = csv.writer(open("/processed_output.csv", "w"),delimiter ='#')
	for key, val in d.items():
   		w.writerow([key.encode('ascii', 'ignore'),val.encode('ascii', 'ignore')])


	read()
