#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"Fetch Dylos averages from KairosDB"

import StringIO, pycurl, json, csv, sys
from datetime import datetime

# Initialize variables
#
URL = "MYURL"
CACERT = "CACERT_PATH"
AUTH = "USERNAME:PASS"
DYLOS_UNITS = ["unit1", "unit2"]
TIME_VAR = ["hours", "minutes", "5minAvg", "days", "hours", \
    "hourAvg", "days", "days", "dayAvg"]
OUT_DIR = "OUT_DIR"

# Functions
#
def prep_query(t_var, u_name, v_var):
    "This prepares a data structure to send in JSON format"
    query_data = {"start_relative": {"value": 1, "unit": t_var[0]}, \
        "metrics" : [{"name": u_name, "tags": {"Bin":"bin1"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]}, \
        {"name": u_name, "tags": {"Bin":"bin2"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]},\
        {"name": u_name, "tags": {"Bin":"bin3"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]},\
        {"name": u_name, "tags": {"Bin":"bin4"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]},\
        {"name": u_name, "tags": {"RH":"rh"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]},\
        {"name": u_name, "tags": {"Temp":"temp"}, "aggregators": [{"name": "avg", "align_sampling": "false", "sampling": {"value": v_var, "unit": t_var[1]}}]}\
        ]}
    return query_data

def send_query(out_file, db_query):
    "This sends out individual queries using pycurl which calls libcurl"
    buf = StringIO.StringIO()
    req = pycurl.Curl()
    req.setopt(req.URL, URL)
    req.setopt(req.CAINFO, CACERT)
    req.setopt(req.USERPWD, AUTH)
    req.setopt(req.POSTFIELDS, json.dumps(db_query))
    req.setopt(req.WRITEDATA, buf)
    req.setopt(req.POST, 1L)
    req.perform()
    req.close()
    
    
    #try to parse the buffer and write it to a file
    try:
        #if buffer is empty write one line of NAs to file
        if (json.loads(buf.getvalue())['queries'][0]['sample_size']==0):
            with open(out_file, "w") as f:
                header1 = 'Timestamp,Bin1,Bin2,Bin3,Bin4,RH,Temp,Flag\n'
                f.writelines(header1)
                f.writelines("NA,NA,NA,NA,NA,NA,NA,NA")
        #otherwise parse buffer
        else:
            num_queries = len(json.loads(buf.getvalue())['queries'])
            l_tags = []
            output_dict = {}
            #loop through all queries (i.e. bin1, bin2, bin3...)
            for q in range(0,num_queries):
                tag_name = json.loads(buf.getvalue())['queries'][q]['results'][0]['tags'].values()[0][0].capitalize()
                l_tags.append(tag_name)
                num_values = len(json.loads(buf.getvalue())['queries'][q]['results'][0]['values'])
                #loop through all values (i.e. [timestamp, value1],[timestamp, value2]...)
                for v in range(0,num_values):
                    key = json.loads(buf.getvalue())['queries'][q]['results'][0]['values'][v][0]
                    value = json.loads(buf.getvalue())['queries'][q]['results'][0]['values'][v][1]
                    #if the key does not exist then add a default value of "NA"
                    if not (key in output_dict):
                        na_list = []
                        for x in range(0,num_queries):
                            na_list.append('NA')
                        output_dict[key] = na_list
                    #save value to the correct offset in list; offsets represent the different queries {ts: [bin1_val, bin2_val...]}
                    output_dict[key][q] = str(value)
            sorted_keys = sorted(output_dict)
            #write values to file
            with open(out_file, "w") as f:
                header1 = 'Timestamp,',','.join(l_tags),',Flag\n'
                f.writelines(header1)
                for item in sorted_keys:
                    item_string = "%d,%s,%s\n" % (item, ','.join(output_dict[item]), "0")
                    f.writelines(item_string)
    #if we cannot parse the values write Err1 to the file
    except:
        with open(out_file, "w") as f:
            header1 = 'Timestamp,Bin1,Bin2,Bin3,Bin4,RH,Temp,Flag\n'
            f.writelines(header1)
            f.writelines("Err1,Err1,Err1,Err1,Err1,Err1,Err1,Err1")

# Loop through each Dylos unit
for dylos in DYLOS_UNITS:
    # Send 5 minute average every 5 minutes, hourly average at the top of every hour
    # daily average at the top of every day
    send_query(OUT_DIR+"/"+dylos+TIME_VAR[2]+".csv", \
        prep_query([TIME_VAR[0], TIME_VAR[1]], dylos, 5))
    if (datetime.utcnow().minute==0):
        send_query(OUT_DIR+"/"+dylos+TIME_VAR[5]+".csv", \
            prep_query([TIME_VAR[3], TIME_VAR[4]], dylos, 1))
        if (datetime.utcnow().hour==0):
            send_query(OUT_DIR+"/"+dylos+TIME_VAR[8]+".csv", \
                prep_query([TIME_VAR[6], TIME_VAR[7]], dylos, 1))