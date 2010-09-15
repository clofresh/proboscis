import sys
import time
from ConfigParser import ConfigParser
from datetime import datetime

import pymongo

config = ConfigParser()
config.read(['proboscis.conf'])

host = config.get('mongodb', 'host')
db_name = config.get('mongodb', 'db')
collection_name = config.get('mongodb', 'collection')
time_key = config.get('fields', 'time')
message_key = config.get('fields', 'message')
timestamp_format = config.get('output', 'timestamp_format')

db = pymongo.Connection(host)[db_name]

if len(sys.argv) > 1:
    filter_query = eval(sys.argv[1])
else:
    filter_query = {}

last_time = list(db[collection_name].find(filter_query, [time_key]).sort(time_key, pymongo.DESCENDING).limit(1))[0][time_key]

while True:
    query = {time_key: {'$gt': last_time}}
    query.update(filter_query)
    
    for row in db[collection_name].find(query).sort(time_key, pymongo.ASCENDING):
        message = row.get(message_key, None)
        timestamp = float(row[time_key])
        
        if message:
            print datetime.fromtimestamp(timestamp).strftime(timestamp_format),
            print message
        last_time = max(last_time, timestamp)
    
    time.sleep(1)
