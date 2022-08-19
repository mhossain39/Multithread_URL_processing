# -*- coding: utf-8 -*-
import csv
import requests

from Queue import Queue
from threading import Thread
import json, sys, time
num_threads = 100


igtoks=list()
igt_index=0

with open('token_and_id_table.csv') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
	   d=dict(igd=row['instagram_id'],ftoken=row['fb_token'])
	   igtoks.append(d)
	

def get_igtok():
	global igt_index, igtoks
	d = igtoks[igt_index]
	if (igt_index+1) == len(igtoks):
		igt_index = 0
	else:
		igt_index+=1
	return d

def process_url(d):
	url='https://url/'+str(d['igd'])+'?fields=business_discovery.username('+d['username']+'){followers_count,follows_count,media_count,ig_id}&access_token='+d['ftoken']
	proxies = {'http': 'http://user:pass@proxyip:port/', 'https': 'http://user:pass@proxyip:port'}
	output = requests.get(url, proxies=proxies, verify=False, timeout=3).text
	if output.find("business_discovery")!=-1:
		data = json.loads(output)
		print (str(data['business_discovery'])+"\n")
	else:
		print("No Business\n")

q = Queue(igtoks)

def do_stuff(q,tn):
	while not q.empty():
		d=q.get()
		process_url(d)
		q.task_done()
with open('username_table.csv') as csvfile:
	reader = csv.DictReader(csvfile)
	rows=list(reader)
		
with open('username_table.csv') as csvfile:
	reader = csv.DictReader(csvfile)
	i=1
	for row in reader:
	   d=get_igtok()
	   y=dict(username=row['username'],igd=d['igd'],ftoken=d['ftoken'],no=i,total=len(rows))	
	   q.put(y)	
	   i +=1

for i in range(num_threads):
    worker = Thread(target=do_stuff, args=(q,i))
    worker.setDaemon(True)	
    worker.start()
q.join()	
