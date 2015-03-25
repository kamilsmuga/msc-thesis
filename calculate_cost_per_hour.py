#!/usr/bin/env python

import matplotlib.pyplot as plot
import csv
from ast import literal_eval
from pyspark import SparkContext, SparkConf, StorageLevel

"""
--------------------------------------------------------
SPARK CONFIGURATION

Used only for standalone execution via bin/spark-submit
--------------------------------------------------------
"""

conf = (SparkConf()
        .setMaster("spark://ksmuga-wsm.internal.salesforce.com:7077")
        .setAppName("Uptime per machine")
        .set("spark.executor.memory", "10g")
        .set("spark.driver.memory", "3g")
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
    day_data = int(splits[1].strip())
    if (day_data != day):
        return

    start_time = int(splits[2].strip())
    end_time = int(splits[3].strip())
    duration = splits[4]
    cpu = splits[5]
    assigned_memory = splits[6]
    max_memory = splits[7]

    tup = (day_data, start_time, end_time, duration, cpu, assigned_memory, max_memory)

    return (machine_id, str(tup))

distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-second-mapping/part*", use_unicode=False)
for x in range(0,3):
    split_by_day = distFile.map(third_mapping)
    split_by_day.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-third-day-" + str(day))
    day++




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
