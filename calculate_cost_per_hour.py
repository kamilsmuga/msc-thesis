#!/usr/bin/env python

import matplotlib.pyplot as plot
import csv
from ast import literal_eval
from pyspark import SparkContext, SparkConf, StorageLevel
from operator import add

"""
--------------------------------------------------------
SPARK CONFIGURATION

Used only for standalone execution via bin/spark-submit
--------------------------------------------------------
"""
SparkContext.setSystemProperty("spark.executor.memory", "28g")
SparkContext.setSystemProperty("spark.default.parallelism", "500")

conf = (SparkConf()
        .setMaster("local")
        .setAppName("Uptime per machine")
        .set("spark.worker.memory", "28g")
        .set("spark.driver.memory", "28g")
        .set("spark.local.dir", "/Users/ksmuga/workspace/data/out"))
sc = SparkContext(conf = conf)


"""
--------------------------------------------------------
FIRST MAPPING TRANSFORMATION 

List tasks durations, timestamps, sampled cpu usage,
canonical me mory and max memory usage. 
--------------------------------------------------------

Schema after transformation:

        1,machine ID,INTEGER,YES
        2,start time,INTEGER,YES
        3,end time,INTEGER,YES
        4,duration INTEGER,YES
        5,sampled CPU usage,FLOAT,NO
        6,assigned memory usage,FLOAT,NO
        7,maximum memory usage,FLOAT,NO

"""

def first_mapping(line):
    splits = line.split(",")
    # key: machine id
    machine_id = int(splits[4])
    # timestamps for start and end time
    start = int(splits[0])
    end = int(splits[1])
    # calculate duration
    duration = end - start
    if duration < 0:
        duration = 0
    cpu = float(splits[19])
    assiged_memory = float(splits[7])
    max_memory = float(splits[10])

    tup = (start, end, duration, cpu, assiged_memory, max_memory)

    return (machine_id, str(tup))


#r1 = sc.textFile("/Users/ksmuga/workspace/data/clusterdata-2011-2/task_usage/part-00000-of-00500.csv")
#r2 = sc.textFile("/Users/ksmuga/workspace/data/clusterdata-2011-2/task_usage/part-00250-of-00500.csv")
#r3 = sc.textFile("/Users/ksmuga/workspace/data/clusterdata-2011-2/task_usage/part-00499-of-00500.csv")
#rrds = sc.union([r1, r2, r3])
"""
rrds = sc.textFile("/Users/ksmuga/workspace/data/clusterdata-2011-2/task_usage/part*")
task_time = rrds.map(first_mapping)
task_time.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-first-mapping")
"""
"""
--------------------------------------------------------
SECOND MAPPING TRANSFORMATION 

Figures out day number and hours from timestamps 
--------------------------------------------------------

Schema after transformation:

        1,machine ID,INTEGER,YES
        2,day,INTEGER,YES
        3,start hour,INTEGER,YES
        4,end hour,INTEGER,YES
        5,duration INTEGER,YES
        6,sampled CPU usage,FLOAT,NO
        7,assigned memory usage,FLOAT,NO
        8,maximum memory usage,FLOAT,NO

"""

def second_mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    start_time = int(splits[1].strip())
    end_time = int(splits[2].strip())
    day = figure_out_day(start_time)
    start_hour = figure_out_hour(start_time, day)
    end_hour = figure_out_hour(end_time, day)   
    duration = splits[3]
    cpu = splits[4]
    assigned_memory = splits[5]
    max_memory = splits[6]

    tup = (day, start_hour, end_hour, duration, cpu, assigned_memory, max_memory)

    return (machine_id, str(tup))

# number of microseconds in a day
micr_secs_in_a_day = 86400000000
# number of microseconds in an hour
micr_secs_in_an_hour = 3600000000
# start offset - 600 seconds in microseconds
start_offset = 600 * 1000000


def figure_out_day(timestamp):
    if (timestamp <= micr_secs_in_a_day + start_offset):
        return 0
    elif (timestamp >= micr_secs_in_a_day * 29 + start_offset):
        return 28

    return round((timestamp + start_offset) / micr_secs_in_a_day)

def figure_out_hour(timestamp, day):
    base = day * 86400000000
    diff = timestamp - base 

    return round(diff / micr_secs_in_an_hour)
"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-first-mapping/part*", use_unicode=False)
task_time = distFile.map(second_mapping)
task_time.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-second-mapping")
"""

"""
--------------------------------------------------------
THIRD MAPPING TRANSFORMATION 

Split data by day usage
--------------------------------------------------------

Schema after transformation:

        1,machine ID,INTEGER,YES
        2,day
        2,start hour,INTEGER,YES
        3,end hour,INTEGER,YES
        4,duration INTEGER,YES
        5,sampled CPU usage,FLOAT,NO
        6,assigned memory usage,FLOAT,NO
        7,maximum memory usage,FLOAT,NO

"""

day = 0

def third_mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    day_data = float(splits[1].strip())
    if (day_data != day):
        return

    start_time = splits[2].strip()
    end_time = splits[3].strip()
    duration = splits[4]
    cpu = splits[5]
    assigned_memory = splits[6]
    max_memory = splits[7]

    tup = (day_data, start_time, end_time, duration, cpu, assigned_memory, max_memory)

    return (machine_id, str(tup))

def third_filter(line):
    splits = line.replace("\"","").replace("(", "").split(",")
    day_data = float(splits[1].strip())
    if (day_data == day):
        return True
    else:
        return False
"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-second-mapping/part*", use_unicode=False)
for x in range(0,30):
    split_by_day = distFile.filter(third_filter)
    split_by_day.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-third-day-" + str(day))
    day += 1
"""

"""
--------------------------------------------------------
FOURTH MAPPING TRANSFORMATION 

Calculate uptime and real workload (based on uptime)
--------------------------------------------------------

Schema after transformation:

        1,machine ID,INTEGER,YES
        2,min_start (minimum start hour),INTEGER,YES
        3,max_end (maximum start hour),INTEGER,YES
        4,total CPU usage,INTEGER,YES
        5,total assigned memory usage,FLOAT,NO
        6,total maximum memory usage,FLOAT,NO

"""

""" 

BELOW FAILS QUICKLY DUE TO OOM 

"""
class Machine:

    def __init__(self, id, start_time=0, end_time=0, real_workload_time=0, total_cpu=0, total_assigned_memory=0, total_max_memory=0):
        _id = id
        _start_time = start_time
        _end_time = end_time
        _real_workload_time = real_workload_time
        _total_cpu = total_cpu
        _total_assigned_memory = total_assigned_memory
        _total_max_memory = total_max_memory

    def __str__(self):
        return _id + "," + str(( _end_time - _start_time, _real_workload_time, _total_cpu, _total_assigned_memory, _total_max_memory ))

    def add_real_workload_time(self, time):
        _real_workload_time += time

    def add_cpu(self, cpu):
        _total_cpu += cpu

    def add_mem(self, memory):
        _total_assigned_memory += memory

    def add_max_memory(self, memory):
        _total_max_memory += memory

machines = {}

def aggregated_uptime(line):
    global machines
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    start_time = float(splits[2].strip())
    end_time = float(splits[3].strip())
    duration_of_workload = int(splits[4].strip())
    cpu = float(splits[5].strip())
    mem = float(splits[6].strip())
    total_mem = float(splits[7].strip())

    if machine_id in machines:
        machines[machine_id]._end_time = end_time
        machines[machine_id].add_real_workload_time(duration_of_workload)
        machines[machine_id].add_cpu(cpu)
        machines[machine_id].add_mem(mem)
        machines[machine_id].add_max_memory(total_mem)
    else:
        machines[machine_id] = Machine(machine_id, start_time, end_time, duration_of_workload, cpu, mem, total_mem)


"""
RRD Spark operations for above ^^


def mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    start = float(splits[2].strip())
    end = float(splits[3].strip())
    return (machine_id, (start, end))

for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-third-day-" + str(x) + "/part*", use_unicode=False)

    min_start = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0], 
                float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[2]))).reduceByKey(lambda a,b: a if a<b else b)

    max_end = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0], 
                float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[3]))).reduceByKey(lambda a,b: a if a>b else b)

    dur = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0], 
                float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[4]))).reduceByKey(lambda a,b: a + b)

    cpu = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0],
                float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[5].strip()))).reduceByKey(lambda a,b: a + b)

    mem = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0],
            float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[6].strip()))).reduceByKey(lambda a,b: a + b)

    total_mem = distFile.map(lambda(x): (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0],
            float(x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[7].strip()))).reduceByKey(lambda a,b: a + b)

    destFile = min_start.join(max_end).join(cpu).join(mem).join(total_mem)
    destFile.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-forth-day-" + str(x))
    day += 1
"""

"""

Calculate number of tasks per machine per day


def mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    counter = 1
    return (machine_id, counter)

for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-third-day-" + str(x) + "/part*", use_unicode=False)

    task_counter = distFile.map(mapping).reduceByKey(add)

    task_counter.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-forth-tasks-day-" + str(x))

"""

""" 
JOIN WITH CPU AND CAPACITY INFO

Schema after transformation (splitted per day)
        1,machine ID,INTEGER,YES
        2,uptime (how many hours machine is on),INTEGER,YES
        3,uptime based on summarized task time,INTEGER,YES
        4,total CPU usage,INTEGER,YES
        5,CPU capacity, INTEGER, YES
        6,total assigned memory usage,FLOAT,NO
        7,total maximum memory usage,FLOAT,NO
        8,MEMORY capacity,FLOAT,YES
        9,NUMBER of tasks per machine per day


"""

def mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    uptime = float(splits[1].strip())
    uptime_total = float(splits[2].strip())
    total_cpu = float(splits[3].strip())
    mem = float(splits[4].strip())
    total_mem = float(splits[5].strip())
    return (machine_id, (uptime, uptime_total, total_cpu, mem, total_mem))

for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-forth-day-" + str(x) + "/part*", use_unicode=False)
    basic_forth = distFile.map(mapping)

    distFileTasks = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-forth-tasks-day-" + str(x) + "/part*", use_unicode=False)
    tasks_forth = distFileTasks.map(lambda x: (x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[0], x.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")[1])) 

    capacity = sc.textFile("/Users/ksmuga/workspace/data/clusterdata-2011-2/machine_events/*", use_unicode=False)
    capacity_map = capacity.map(lambda x: (x.split(",")[1], (x.split(",")[4], x.split(",")[5])))

    joined_file = basic_forth.join(capacity_map).join(tasks_forth)
    joined_file_distinct = joined_file.distinct()
    joined_file_distinct.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-fifth-day-" + str(x))
    day += 1
"""

GET ONLY UPTIME


def only_uptime_mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    uptime = float(splits[2].strip()) - float(splits[1].strip())
    return (machine_id, uptime)

for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-forth-day-" + str(x) + "/part*", use_unicode=False)
    only_uptime = distFile.map(only_uptime_mapping)
    distinct_uptime = only_up  time.distinct()
    distinct_uptime.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-fifth-uptime-only-day-" + str(x))

"""

"""
--------------------------------------------------------
DEPRECATED TRANSFORMATION 

Calculate aggregated time spent on tasks per machine
Produces K,V - (machine id, aggregated time)
--------------------------------------------------------
"""


def calc_aggregated_task_time(line):
    """
    Map function to calculate total time spent on task 
    Returns tuple: (machine, task_time)

    Schema for task usage

        1,start time,INTEGER,YES
        2,end time,INTEGER,YES
        3,job ID,INTEGER,YES
        4,task index,INTEGER,YES
        5,machine ID,INTEGER,YES
        6,CPU rate,FLOAT,NO
        7,canonical memory usage,FLOAT,NO
        8,assigned memory usage,FLOAT,NO
        9,unmapped page cache,FLOAT,NO
        10,total page cache,FLOAT,NO
        11,maximum memory usage,FLOAT,NO
        12,disk I/O time,FLOAT,NO
        13,local disk space usage,FLOAT,NO
        14,maximum CPU rate,FLOAT,NO
        15,maximum disk IO time,FLOAT,NO
        16,cycles per instruction,FLOAT,NO
        17,memory accesses per instruction,FLOAT,NO
        18,sample portion,FLOAT,NO
        19,aggregation type,BOOLEAN,NO
        20,sampled CPU usage,FLOAT,NO

    """
    splits = line.split(",")
    start = splits[0]
    end = splits[1]
    machine = splits[4]
    result = int(end) - int(start)
    if result < 0:
        return (machine, 0)
    else: 
        return (machine, result)

"""
distFile = sc.textFile("/home/ks/workspace/data/clusterdata-2011-2/task_usage/part-*")
task_time = distFile.map(calc_aggregated_task_time)
count = task_time.reduceByKey(lambda a, b: a + b)
count.saveAsTextFile("/home/ks/workspace/data/spark-out/machine_to_aggregated_task_times")
"""

""" 
------------------
PRICING INPUT DATA
------------------
"""
# values for size: cc2.8xlarge region: us-east
# 1 year reservation
c_name = 'cc2.8xlarge'
c_light = { "upfront" : "1762", "perhour" : "0.904" }
c_medium = { "upfront" : "4146", "perhour" : "0.54" }
c_heavy = { "upfront" : "5000", "perhour" : "0.361" }

# values for size: m2.4xlarge region: us-east
# 1 year reservation
m_name = 'm2.4xlarge'
m_light = { "upfront" : "1088", "perhour" : "0.676" }
m_medium = { "upfront" : "2604", "perhour" : "0.52" }
m_heavy = { "upfront" : "3156", "perhour" : "0.272" }

# values for size: hs1.8xlarge region: us-east
# 1 year reservation
h_name = 'hs1.8xlarge'
h_light = { "upfront" : "3968", "perhour" : "2.24" }
h_medium = { "upfront" : "9200", "perhour" : "1.38" }
h_heavy = { "upfront" : "11213", "perhour" : "0.92" }

""" 
-----------------------------
INSTANCE ENTITY UTILITY CLASS
-----------------------------
"""
class Entity:
    def __init__(self, name, light, medium, heavy):
        self.name = name
        self.light = light
        self.medium = medium
        self.heavy = heavy
        self.ligth_cph = [ self._calc_cost_per_hour(self.light, x) for x in range(1,25) ]
        self.medium_cph = [ self._calc_cost_per_hour(self.medium, x) for x in range(1,25) ]
        self.heavy_cph = [ self._calc_cost_per_hour(self.heavy, x) for x in range(1,25) ]
        self.i_light_to_medium = self._calc_intersection_point(self.ligth_cph, self.medium_cph)
        self.i_medium_to_high = self._calc_intersection_point(self.medium_cph, self.heavy_cph)
        self.i_light_to_high = self._calc_intersection_point(self.ligth_cph, self.heavy_cph)

    def _calc_cost_per_hour(self, mode, hours_per_day):
        upfront = int(mode["upfront"])
        cost_per_hour = float(mode["perhour"])
        cph = (upfront + (365 * cost_per_hour * hours_per_day)) / (365 * hours_per_day)
        return cph   

    def _calc_intersection_point(self, first, second):
        for i in range(len(first)):
            if (second[i] <= first[i]):
                return i

    def plot(self):
            t = range(24)
            light_plot = plot.plot(t, self.ligth_cph)
            medium_plot = plot.plot(t, self.medium_cph)
            heavy_plot = plot.plot(t, self.heavy_cph)

            plot.legend(['light', 'medium', 'heavy'])
            plot.savefig(self.name + '.png')
            plot.clf()

    def to_csv(self):
        with open(self.name + '.csv', 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(['light', 'medium', 'heavy'])
            for i in range(24):
                csvwriter.writerow([self.ligth_cph[i], self.medium_cph[i], self.heavy_cph[i]])

def main():
    entities = [ Entity(c_name, c_light, c_medium, c_heavy), Entity(m_name, m_light, m_medium, m_heavy), Entity(h_name, h_light, h_medium, h_heavy) ]

    for e in entities:
        #e.plot()
        #e.to_csv()
        print e.name
        print e.i_light_to_medium
        print e.i_medium_to_high
        print e.i_light_to_high
        

#if __name__ == "__main__":
#    main()
