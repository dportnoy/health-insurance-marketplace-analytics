#!/usr/bin/env python
# -*- coding: utf-8 -*-
#==============================================================================
#= Intelligent download of PUFs
#= ----------------------------------------------------------------------------
#= Created: By David Portnoy on 2014/02/24
#= Purpose: Examine files available online and only downlaod if changed.
#==============================================================================


from pymongo import MongoClient
from bson import ObjectId
import datetime
import requests


# Globals
global debug



# FIXME: last_response should come from DB
def check_file_changed(url_file,last_response):
    
    # Check valid URL
    # return -1  # Indicates error

    response = requests.head(url_file)

    # Simulate saving prior response
    # For now last_response is global
    file_changed = False
    for key, last_value in last_response.items():
        if not response.headers[key] == last_value:
            file_changed = True
            break  # No need to check any more 

    print("---\nfile_changed: "+str(file_changed))

    #Save prior response
    last_response = {'Last-Modified': response.headers['Last-Modified'],
                     'ETag' : response.headers['ETag'],
                     'Content-Length': response.headers['Content-Length']
                    }
    
    if file_changed:
        return url_file  # Indicates changed
    else:
        return False
    


def check_file_changed(url_file,crawl_log):
    # Save last file loaded
    if not crawl_log.find_one({'url':url_file,'header': last_response}): 
        response_doc = {
            'url':url_file,
            'date': datetime.datetime.utcnow(),
            'header': last_response    
        }
        print("---\nInserting: "+str(document)) 
        document_id = collection.insert_one(response_doc).inserted_id
        print(document_id)
    else:
        print('Already exists')
    print(collection.count())

        
    print("---\nfile_changed: "+str(file_changed))

    #Save prior response
    last_response = {'Last-Modified': response.headers['Last-Modified'],
                     'ETag' : response.headers['ETag'],
                     'Content-Length': response.headers['Content-Length']
                    }
    

    
    


def main():
    client = MongoClient()
    db = client.crawler  # Equiv to relational database
    crawl_log = db.crawl_log  # Equiv to table
    
    check_file_changed(url_file,crawl_log)


    
'''
#=== Test ===
last_response = {'ETag': '"667dbf1227e0a2ace7c89f4dc143c647:1450821775"', 'Content-Length': '28623', 'Last-Modified': 'Tue, 22 Dec 2015 22:02:55 GMT'}
url_file = 'http://download.cms.gov/marketplace-puf/2016/machine-readable-url-puf.zip'
print( check_file_changed(url_file,last_response))
'''