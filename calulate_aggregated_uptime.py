#!/usr/bin/env python

import json
import csv

MACHINES_CSV = 'machine_events/part-00000-of-00001.csv'

# machine id to timestamp and event mapping
machines = {}

# total number of machines
machines_count = 0

# last timestamp
last_timestamp = 0

with open(MACHINES_CSV, 'rb') as machines_csv:
    machine_reader = csv.reader(machines_csv, delimiter=',')
    for row in machine_reader:
        machine_id = row[1]
        event = int(row[2])
        timestamp = int(row[0])
        if timestamp > last_timestamp:
            last_timestamp = timestamp
        if not machine_id in machines:
            machines[machine_id] = []
        machines[machine_id].append({timestamp: event})

machines_count = len(machines)
print 'Total count of machines: %s' % machines_count
print 'Last time stamp: %s' % last_timestamp

"""
fileobj = open('test', 'w')
try:
    json.dump(machines, fileobj)
except Exception:
    raise
finally:
    fileobj.close()
"""
