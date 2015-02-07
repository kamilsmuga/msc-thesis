#!/usr/bin/env python

import matplotlib.pyplot as plot
import csv
from pyspark import SparkContext, SparkConf

"""
-------------------
SPARK CONFIGURATION
-------------------

--------------------------------------------------------
Used only for standalone execution via bin/spark-submit
--------------------------------------------------------
conf = (SparkConf()
        .setMaster("local")
        .setAppName("Uptime per machine")
        .set("spark.executor.memory", "10g")
        .set("spark.local.dir", "/home/ks/workspace/data/spark-out"))
sc = SparkContext(conf = conf)
"""

def calc_task_time(line):
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
    else return (machine, result)

distFile = sc.textFile("/home/ks/workspace/data/clusterdata-2011-2/task_usage/part-00000-of-00500.csv")
task_time = distFile.map(calc_task_time)

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
        

if __name__ == "__main__":
    main()
