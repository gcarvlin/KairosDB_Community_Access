#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"Fetch Dylos averages from KairosDB"

import StringIO, pycurl, json, csv
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

    with open(out_file, "w") as json_out:
        json_out.write(buf.getvalue())

# Loop through each Dylos unit
for dylos in DYLOS_UNITS:
    # Send 5 minute average every 5 minutes, hourly average at the top of every hour
    # daily average at the top of every day
    send_query(OUT_DIR+"/"+dylos+TIME_VAR[2]+".json", \
        prep_query([TIME_VAR[0], TIME_VAR[1]], dylos, 5))
    if (datetime.utcnow().minute==0):
        send_query(OUT_DIR+"/"+dylos+TIME_VAR[5]+".json", \
            prep_query([TIME_VAR[3], TIME_VAR[4]], dylos, 1))
        if (datetime.utcnow().hour==0):
            send_query(OUT_DIR+"/"+dylos+TIME_VAR[8]+".json", \
                prep_query([TIME_VAR[6], TIME_VAR[7]], dylos, 1))