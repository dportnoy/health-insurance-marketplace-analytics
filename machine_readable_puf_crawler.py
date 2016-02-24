
import shutil
import zipfile
import pandas as pd
# import xlrd  # library for reading xlsx, but can use pandas.read_excel() 
import json
import requests
from urllib import parse
import os

global debug



def get_unique_urls():
    ps = df['URL Submitted'].value_counts()
    for i, v in ps.iteritems():
        print(i)
        print(v)


def read_json_url(url):
    
    global global_error
    global_error = ''
    
    try:
        r = requests.get(url, allow_redirects=True)
    except requests.exceptions.RequestException as error:    # This is the correct syntax
        print (error)
        global_error = str(error)
        return None, None

    if r.status_code == 200:
        # Note: As of 2/17/2016 Content-type is sometimes not json
        if not 'json' in r.headers.get('content-type',''):
            global_error += 'content-type should not be: '+ r.headers.get('content-type','missing')+"'\n"
        if debug: print("Good!")
        json_data = json.loads(r.text)
        metadata = save_response_metadata(url, r)   # Need elapsed.total_seconds()  
        return (json_data, metadata)
    else:
        if debug: print("Not good! Status code:" + str(r.status_code) + "    Content-type: " + r.headers.get('content-type',''))
        return None, None
    return




def save_response_metadata(url, response):
    metadata = {}
    metadata['status_code'] = response.status_code
    metadata['history']     = response.history
    metadata['url']         = response.url
    metadata['_diff_url']   = (response.url == url)
    metadata['headers']     = response.headers
    metadata['elapsed']     = response.elapsed
    metadata['total_secs']  = response.elapsed.total_seconds()
    metadata['file_name' ]  = os.path.basename(parse.urlparse(url).path)

    return metadata


 
def load_zip_to_df(full_dst):
    zfile = zipfile.ZipFile(full_dst)
    zfile.extractall()
    
    global df

    # Read into Pandas DF
    for file_name in zfile.namelist():
        print("Loading: "+file_name)
        df = pd.read_excel(file_name)
        df.as_matrix()
        print("Records loaded: "+str(len(df)))
        break
        
    return





def count_url_items(json_url, max_depth=1, response_times = [], parent_key = 'index_url_items'):

    if debug: print("\n\n\n--------------------------")
    if debug: print("index  = "+str(index))
    if debug: print("json_url  = "+str(json_url))
    if debug: print("max_depth = "+str(max_depth))
    
    if max_depth == 0:
        if debug: print("Exiting")
        return 0
    
    max_depth -= 1
    
    json_data, metadata = read_json_url(json_url)
    
    if json_data == None:
        if debug: print("--- Nonetype!")
        return 0

    response_times.append(metadata['total_secs'])     # List of times to calculate
    df.loc[index, 'avg_response_time'] = str(response_times)  # Update when it changes; Can't assign dict to df value
    
    if debug: print("type(json_data) = "+str(type(json_data)))
    
    if isinstance(json_data, dict):
        for dict_key, dict_value in json_data.items():
            if isinstance(dict_value, list):
                count = len(dict_value)
                if debug: print("# of items for "+ dict_key+" : " + str(count))
                df.loc[index, dict_key] = count
                if max_depth >= 1:
                    for list_key, list_value in enumerate(dict_value):
                        count = count_url_items(list_value, max_depth, response_times, dict_key+'_items')
                        #if debug: print(list_value)
            else:
                count = 1
                if debug: print("# of items for "+ dict_key+" : 1")
                df.loc[index, dict_key] = count
                if max_depth >= 1:
                    count = count_url_items(dict_value, max_depth, response_times, dict_key+'_items')
                    return count
                #if debug: print(dict_value)          
    elif isinstance(json_data, list):
        count = len(json_data)
        if debug: print("# of items for "+ str(parent_key) +" : " + str(count))
        df.loc[index, str(parent_key)] = count

    else:
        if debug: print("Error: json is not dictionary")
        return 0
    return
        


# This is a custom alternative to use of parse.urlparse(url)
# because sometimes it doesn't work well
# Simple custom rules for checking validity
def validate_url_custom(url):
    url = url.strip()
    returned_url = ''
    if url.find(' ')==-1 and url.find('.')>0 and url.find('.')<len(url)-1: 
        if not url.lower().find('http')==0:
            # Protocol not specified, but no spaces and with periods.  Try adding http://"
            url = 'http://' + url
    else:
        # Bad! Spaces or no periods"
        url = ''
        
    return url
        

def save_to_excel():    
    writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
    df.to_excel(writer,'Sheet1')
    writer.save()
    return


from IPython.display import display, clear_output
import sys


def print_same_line(print_str):
    
    '''
    #=== stdout method ===
    sys.stdout.write('\r'+print_str)
    sys.stdout.flush()
    ''' 
    
    #=== iPython method ===
    if not debug:
        clear_output(wait=True)
        print(print_str+"\n")
        sys.stdout.flush()
    else:
        print(print_str+"\n")
    
    return
    
    
def main(start_row=0 , end_row=9, max_depth = 1):
    global df
    global index
    global global_error
    global_error = ''
    # Test
    src = '../DDOD-HealthData.gov/snapshots/'
    src += 'machine-readable-url-puf_2016-02-11.zip'
    dst = '.'
    print(src)
    print(dst)
    full_dst = shutil.copy2(src, dst)  # copy2 preserves metadata
    load_zip_to_df(full_dst)
    
    #:=== Initialize columns
    df['Status'] = '' # Add column now to prevent errors
    df['avg_response_time'] = '' # Add column now to prevent errors
    
    #:=== If dictionaries are better
    #url_dict = pd.DataFrame.to_dict(df,'records')

    

    # Loop through all rows
    #=====  Dataframe version ======
    for index, row in df.loc[start_row:end_row].iterrows():

        print_same_line("Reading row:  "+str(index)+"\n")
        
        #print(index)
        #print(row)
        url_submit = row['URL Submitted']
        url_clean = validate_url_custom(url_submit)
        if url_clean:
            row_status = count_url_items(url_clean, max_depth, [])
            df.loc[index, 'Status'] = 'Read' if (global_error == '') else global_error  # Can't assign dict
        else:
            df.loc[index, 'Status'] = "Missing"
            
        if df.loc[index, 'avg_response_time']:
            time_list    = json.loads(df.loc[index, 'avg_response_time'])
            time_average = format(pd.np.mean(time_list),'.3f')
            df.loc[index, 'avg_response_time'] = time_average
            
        if debug: 
            print('\n\n----- Row -----')
            print(df.iloc[[index]])
            
        if index >= end_row: break

    save_to_excel()
        
    print("\nDone!")
    return