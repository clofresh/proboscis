import time
from datetime import datetime

import pymongo

db = pymongo.Connection().mongolog

time_key = 'created'
message_key = 'msg'

last_time = list(db.log.find({}, [time_key]).sort(time_key, pymongo.DESCENDING).limit(1))[0][time_key]

while True:
    for row in db.log.find({time_key: {'$gt': last_time}}).sort(time_key, pymongo.ASCENDING):
        message = row.get(message_key, None)
        if message:
            print datetime.fromtimestamp(float(row[time_key])).strftime('%Y-%m-%d %H:%M:%S.%f:\t'),
            print message
        last_time = max(last_time, row[time_key])
    
    time.sleep(1)
