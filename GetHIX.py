#==============================================================================
#= Community-contributed code by https://github.com/BAP-Jeff on 2/19/2016
#=------------------------
#= Purpose: Extract formularies provided by QHPs by crawling websites starting
#=          with CMS/CCIIO marketplace PUF. 
#==============================================================================
import json
import sys
import os
import urllib2
import MySQLdb
from datetime import datetime
from pprint import pprint
from collections import OrderedDict

# Open database connection
db = MySQLdb.connect(host="blah")

#I want to Timestamp things so let's grab it
timeStamp = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
print timeStamp

#Setting up the Filenames
HIXJSONLog = "HIXJSONLog"+"_"+timeStamp+".log"
HIXJSONError = "HIXJSONError"+"_"+timeStamp+".log"
HIXForm = "HIXFormulary"+"_"+timeStamp+".pip"

# execute SQL query using execute() method.
# prepare a cursor object using cursor() method
cursor = db.cursor()
#probably should change this to a stored procedure at some point
sql = "select URLSubmitted from BAP.CMS_Marketplace_Machine_Readable_URLs where Status='X'"
Y = 0

# Fetch rows.
cursor.execute(sql)
results = cursor.fetchall()

#Iterate over the Database results
for row in results:

	#Grab the URL from the cursor
	url = row[0]
	
	#Try to get the URL, some of these are bogus
	try:
		response = urllib2.urlopen(url)
		
		#Pull out the formulary URLS, can be more than one per company
		jsonStart = json.load(response)
		formularyURLs = jsonStart['formulary_urls']
		
	#If we fail, jump out of the loop	
	except Exception as e:
		print e
		print 'Bad URL ' + url
		continue
	
	for formularyURL in formularyURLs:

		#Go grab the individual Formularies
		try:
			response = urllib2.urlopen(formularyURL)
			
			# read from request while writing to file
			file = "HIXJSON"+str(Y)+"_"+timeStamp+".json"
			Y = Y + 1
			
			#I want to store the file locally	
			with open(file, 'w') as f: 
				f.write(response.read())
			
			#Keep a file that links filename to URL	
			with open(HIXJSONLog, "a") as f:
				string = file + "|" + url + "|" + formularyURL + "\n"
				string_for_output = string.encode('utf8')
				f.write(string_for_output)
			
		except Exception as e:
			print 'Bad Formulary URL '+formularyURL
			print e
			
			with open(HIXJSONError, "a") as f:
				string = url + "\n"
				string_for_output = string.encode('utf8')
				f.write(string_for_output)
				
			continue
		
# disconnect from server
db.close()



