# -*- coding: utf-8 -*-
import csv
import requests, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from Queue import Queue
from threading import Thread
import json, sys, time
import mysql.connector as mysql

mysql_user = ""		#MySQL connection setting
mysql_pass = ""
mysql_database = ""
num_threads = 100	## Number of threads

connection = mysql.connect(
    host = "localhost",
    user = mysql_user,
    passwd = mysql_pass,
    database = mysql_database
)
connection.autocommit = True
cursor = connection.cursor()




igtoks=list()
igt_index=0
proxies=list()
proxy_index=0

sql = "SELECT instagram_id,fb_token FROM token_and_username "
cursor.execute(sql)
reader = cursor.fetchall()
for row in reader:
   d=dict(igd=row[0],ftoken=row[1])
   igtoks.append(d)

sql = "SELECT distinct(proxy) FROM user_ig_proxy "
cursor.execute(sql)
reader = cursor.fetchall()
for row in reader:
   proxies.append(row[0])

def get_proxy():
	global proxy_index, proxies
	d = proxies[proxy_index]
	if (proxy_index+1) == len(proxies):
		proxy_index = 0
	else:
		proxy_index+=1
	return d

def get_igtok():
	global igt_index, igtoks
	d = igtoks[igt_index]
	if (igt_index+1) == len(igtoks):
		igt_index = 0
	else:
		igt_index+=1
	return d

def process_url(d,cursor):
	url='https://url/'+str(d['igd'])+'?fields=business_discovery.username('+d['username']+'){followers_count,follows_count,media_count,ig_id}&access_token='+d['ftoken']
	p = get_proxy()
	proxy = {'http': p, 'https': p}
	try:
		output = requests.get(url, proxies=proxy, verify=False, timeout=3).text
	except Exception,err:
		print err
		output = ""

	if output.find("business_discovery")!=-1:
		data = json.loads(output)
		print (str(data['business_discovery'])+"\n")
		sql_insert = "INSERT INTO ninjalitics_business_user(idninjalitics_user,username,id_user,date) VALUES(NULL,%s,%s,NOW());"
		cursor.execute(sql_insert, (d['username'],d['igd']))
			#connection.commit()

	else:
		print("No Business\n")


q = Queue(maxsize=0)
def do_stuff(q,tn):
	connection = mysql.connect(
	    host = "localhost",
	    user = mysql_user,
	    passwd = mysql_pass,
	    database = mysql_database
	)
	connection.autocommit = True
	cursor = connection.cursor()
	while not q.empty():
		d=q.get()
		process_url(d,cursor)
		q.task_done()
	connection.close()

def main():					#main function that runs all thread
	sql = "SELECT username FROM username"
	cursor.execute(sql)
	reader = cursor.fetchall()
	i=1
	rc = cursor.rowcount
	for row in reader:
	   d=get_igtok()
	   y=dict(username=row[0],igd=d['igd'],ftoken=d['ftoken'],no=i,total=rc)	
	   q.put(y)	
	   i+=1
	connection.close()	
	for i in range(num_threads):
	    worker = Thread(target=do_stuff, args=(q,i))
	    worker.setDaemon(True)	

	    worker.start()
	q.join()
if __name__ == "__main__":

    main()
