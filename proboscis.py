import time
from datetime import datetime

import pymongo

db = pymongo.Connection().mongolog

last_time = list(db.log.find({}, ['time']).sort('time', pymongo.DESCENDING).limit(1))[0]['time']

while True:
    for row in db.log.find({'time': {'$gt': last_time}}).sort('time', pymongo.ASCENDING):
        print datetime.fromtimestamp(row['time']).strftime('%Y-%m-%d %H:%M:%S.%f:\t'),
        print row['message']
        last_time = max(last_time, row['time'])
    
    time.sleep(1)