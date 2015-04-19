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
"""
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
"""

GET ONLY UPTIME

"""


def only_uptime_mapping(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    machine_id = splits[0].strip()
    uptime = float(splits[2].strip()) - float(splits[1].strip())
    return (machine_id, uptime)
"""
for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-forth-day-" + str(x) + "/part*", use_unicode=False)
    only_uptime = distFile.map(only_uptime_mapping)
    distinct_uptime = only_uptime.distinct()
    distinct_uptime.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-fifth-uptime-only-day-" + str(x))

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


"""

GET ONLY CPU = 0.5 and MEMORY = (0.5 OR 0.25)

Schema after transformation (splitted per day)
        1,machine ID
        2,uptime (how many hours machine is on)
        3,distance from optimality point
        4,total cost per day
        5,cpu usage
        6,memory usage
        7,number of tasks
        8,MEMORY capacity
        9,CPU capacity

"""

vm = Entity(c_name, c_light, c_medium, c_heavy)

def cpu_and_mem(line):
    ip = vm.i_medium_to_high
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")

    mach_id = splits[0].strip()
    start = float(splits[1].strip())
    end = float(splits[2].strip())

    cpu_usage = float(splits[3].strip())
    memory_usage = float(splits[4].strip())

    up = end - start

    cpu_cap = splits[6].strip()
    mem_cap = splits[7].strip()
    tasks = splits[8].strip()

    if cpu_cap != "":
        cpu_cap = float(cpu_cap)

    if mem_cap != "":
        mem_cap = float(mem_cap)

    cost = 0
    distance = 0
    if (cpu_cap == 0.5 and mem_cap > 0.24):
        cph = vm.heavy_cph[int(up) - 1]
        cost = cph * up
        distance = up - ip
    else:
        cph = vm.medium_cph[int(up) - 1]
        cost = cph * up
        distance = ip - up

    return (mach_id, (up, distance, cost, cpu_usage, memory_usage, tasks, cpu_cap, mem_cap))
"""
for x in range(0, 30):
    distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-fifth-day-" + str(x) + "/part*", use_unicode=False)
    heavies = distFile.map(cpu_and_mem)
    heavies.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-sixth-day-" + str(x))

"""

"""" CALC TOTAL COST 

Results:
1. total cost
2. total cost for heavy
3. total cost for medium 
4. total uptime 
"""
def cost(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")
    cost = float(splits[3].strip())
    return cost

def cost_for_heavy(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    cpu_cap = splits[7].strip()
    mem_cap = splits[8].strip()
    if cpu_cap != "":
        cpu_cap = float(cpu_cap)
    if mem_cap != "":
        mem_cap = float(mem_cap)
    if (cpu_cap == 0.5 and mem_cap > 0.24):
        return True
    else:
        return False

def total_uptime(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    up = float(splits[1].strip())
    return up

def total_distance(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    distance = float(splits[2].strip())
    return distance  

def total_cpu(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    cpu = float(splits[4].strip())
    return cpu

def total_mem(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    mem = float(splits[5].strip())
    return mem

def total_tasks(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    tasks = float(splits[6].strip())
    return tasks

"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-sixth-day-*/*", use_unicode=False)
total_cost = distFile.map(cost).reduce(add)
heavy_cost = distFile.filter(cost_for_heavy).map(cost).reduce(add)
total_up = distFile.map(total_uptime).reduce(add)
total_dis = distFile.map(total_distance).reduce(add)
cpu = distFile.map(total_cpu).reduce(add)
mem = distFile.map(total_mem).reduce(add)
tasks = distFile.map(total_tasks).reduce(add)
"""

def filter_negatives(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    distance = float(splits[2].strip())
    if distance < 0:
        return True
    else:
        return False
"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-sixth-day-*/*", use_unicode=False)
negatives = distFile.filter(filter_negatives)
negatives.saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-seventh")

distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-seventh/*", use_unicode=False)
number_of_machines_to_tune = distFile.map(lambda x: x.split(",")[0]).distinct().collect()
"""
# count 
"""
distFile.map(lambda x: (x.split(",")[0], 1)).reduceByKey(add).saveAsTextFile("/Users/ksmuga/workspace/data/out/transformation-eight")
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-eight", use_unicode=False)
list_of_negatives = distFile.filter(lambda x: x.split(",")[1] == " 29)").collect()
"""

negatives_ids = [4820139637, 4815459946, 4837752655, 4820430817, 5290274741, 5100834323, 4820077486, 4820103513, 706377, 4802043170, 4802146191, 4820138112, 4820095887, 4802124009, 4820247043, 4820359879, 4802848681, 4873966196, 4820134564, 4820222658, 4820318101, 5015681460, 2197057358, 4820223984, 4820233415, 4820072517, 4820447540, 4820136839, 4820155266, 4820157076, 38744085, 4820094770, 4802487721, 1390803716, 4820236711, 3631094036, 4820134769, 4820134385, 1390856073, 4820115855, 4820096543, 4820362701, 4802476128, 3631314252, 4802151519, 3337972745, 4820072958, 4820177057, 3338266372, 4820093043, 4820309854, 1390772944, 4802506090, 4820246091, 4820155700, 4820093885, 4820022174, 4820235500, 4820107221, 4820185414, 4819997869, 4820358468, 4820094424, 3696086053, 4802863353, 4853929305, 6569657, 4802474396, 4842960836, 4802467653, 5777330612, 3718181567, 4802146889, 4820222428, 5015802270, 4828895378, 3631294705, 4820059173, 4820381786, 4802132957, 4820134488, 1390896856, 1390936667, 4820028357, 3338244020, 4820140717, 4820136172, 4820157300, 4820347938, 1391015405, 4820315197, 4820106869, 4873114565, 4820225987, 1436489701, 4820186483, 4820227126, 4802136602, 4820025580, 4820216876, 4820096449, 765613, 3454292678, 4817603466, 4820137403, 4820095595, 4820058969, 4820096567, 1390932404, 4820310861, 4820338069, 4820136369, 4824432155, 2433433753, 4820061208, 4820013687, 4820134805, 4988455129, 4820073668, 4820032202, 4802863150, 4820073848, 4820210891, 4802506677, 4820029036, 4820110723, 4820182799, 4820358467, 4802910981, 4820361969, 4802122622, 1093693, 4820003183, 4820092048, 4820181479, 3359093306, 4821070690, 4820239559, 765912, 4802978415, 4820149990, 5015725340, 4820020938, 4820137260, 4820156549, 4820022347, 2616945439, 4802904615, 5015656043, 4802514029, 4820059440, 5015688164, 4820137445, 4820359794, 4820215258, 4820034742, 4874096349, 4820368543, 4820373138, 4820096817, 3338280002, 4820258292, 4820389840, 4820234896, 4819998188, 4802476989, 4820338371, 4820021252, 5118782424, 4820076223, 4820250774, 4820073870, 4820108770, 4820155450, 4802864573, 3631256022, 4143950757, 4820005509, 3631292570, 4802139634, 4802477432, 4802491967, 4043119987, 4820236249, 4820115870, 6264344062, 4820367467, 3890183003, 1390983185, 1390840508, 4820092682, 6000919152, 4820256950, 4820183560, 4820285492, 4820223849, 4820101410, 4820092870, 4820096477, 4820096649, 3453254128, 4820227700, 4820138090, 4820250647, 4820180887, 4820074688, 3739340729, 4820017067, 4820137356, 4820285633, 4874280806, 4820155203, 4820248072, 4820059525, 4820289779, 4867745523, 4820139951, 4820059668, 5017317661, 4820076231, 4820029995, 5015836622, 4820095848, 4802864268, 4881207024, 4820026480, 682395, 4874535772, 4820094953, 4820097362, 4820107181, 4820157409, 3337817283, 4820028537, 4820183066, 4820097048, 3250659235, 4820140755, 4802143174, 4802892015, 4820854840, 4820358249, 4820313419, 4820155784, 4820436037, 4820097379, 4820310876, 4820095468, 4820226730, 4820091923, 4820359613, 4802519329, 4820076287, 4802905480, 5015788232, 4820240231, 4819998974, 4820136827, 4820180814, 4802514053, 3311874756, 4802517766, 82754515, 4820445919, 2567915203, 4820072867, 4820910848, 4802506091, 4820233297, 4820138006, 4802905544, 4802483639, 4802484114, 4820228787, 4820240534, 4802503736, 4820002342, 4820136643, 4820365692, 4820022100, 294994014, 4802842149, 640736354, 4820110436, 3453117765, 4802120401, 4820221395, 5137002965, 4820097391, 4820437124, 4820204869, 4802889220, 4820106260, 4820234954, 3631179812, 4802141783, 4820092871, 3631103217, 4820267435, 4873175358, 6565000, 4802103040, 4820074200, 4802127438, 4820109218, 4820346650, 4820017069, 4820223806, 3739365722, 4820361957, 4820356438, 4820076565, 4820020410, 4820135116, 3311442104, 4820076642, 97965637, 4820099551, 4820076711, 4820253490, 3338303846, 4802863684, 4820187800, 4874547573, 3338306187, 5168862796, 4820359129, 4820072706, 4820072401, 2567993455, 227390250, 4820223735, 4802952927, 4820139381, 3338287454, 4820170947, 4820262802, 4820019450, 4820254606, 4802844117, 4802887274, 4819958352, 4820177075, 3251096906, 4802079544, 4820095760, 1391159347, 4820428784, 5015682200, 4820152618, 4874113956, 1390987356, 5118955112, 4820075588, 4802850503, 5015706926, 1390915544, 4844666475, 4820079501, 4802892103, 3996132249, 4820092151, 4820024112, 5069621014, 4820235521, 4802473813, 4820152582, 4802041305, 2840510370, 2586575798, 1390834671, 2978903478, 4819999495, 1391046536, 4820315928, 4802887596, 4820103455, 4820274057, 4820426474, 4820248525, 4802491016, 4820028096, 4820200692, 4820073645, 3337948963, 4820139301, 4820155326, 3338239375, 4820095049, 4820365964, 4802498578, 4027200724, 4820218075, 4820184999, 4802491044, 4820017998, 4820356727, 3337872170, 5015667066, 4820028570, 4820191072, 4820029968, 4820241801, 3337970143, 4820390019, 4874252578, 4820060350, 4820338434, 4820212398, 4820326459, 4819990193, 4820156550, 4802489439, 4820072821, 4820382004, 4820449042, 4820031883, 4820074350, 4820144491, 4820091924, 4820268323, 5303911249, 4802146470, 4820294844, 662216, 336038648, 4820181362, 4820236868, 705638, 4820457161, 4820309550, 4820058941, 4820074114, 4820221388, 4820131094, 4893347381, 400946306, 3631265322, 1092694, 4820159474, 3337876230, 4802495635, 4820092463, 4820093536, 4820096633, 4820184623, 4820350140, 4820103578, 765896, 3631285943, 185027523, 5015788297, 4820854636, 4820157095, 4820381954, 4820139430, 4820020161, 3337968809, 4820794786, 4820096768, 4820011766, 4802039830, 4802120333, 3630727628, 4820008783, 765966, 4820102641, 4802146986, 4820110437, 4802910787, 4802492155, 4820172023, 4820185434, 4820099485, 4820261129, 4802130219, 4820072458, 4802474338, 4820417270, 4820094627, 4820138060, 38743463, 4820002127, 3631183884, 5015626000, 4802517677, 5015607972, 5015887240, 4820093730, 3925869700, 4820095525, 4820094425, 3337940322, 3772503877, 1094459, 4820059719, 4802136717, 4846345465, 4820028262, 4820390100, 63693701, 4820095517, 4938563404, 4820251359, 4820059212, 4820152351, 4820093460, 4820220492, 905607, 4802151072, 4820140083, 3337839998, 4820234513, 309375234, 4820150108, 1436688656, 4820217961, 4820240884, 4820139461, 4820180964, 1390814016, 564444462, 4802853535, 4820137538, 4820318122, 4820226557, 4820107319, 4820139883, 4820251792, 4820243843, 4820252642, 4802132527, 4820394062, 4820096260, 4802484241, 4820099550, 5017364115, 1390853332, 4820258637, 4820155405, 4802479630, 4820181018, 3631105600, 4820218652, 1391008303, 4820391494, 344839454, 4820453791, 4820059749, 4820157039, 216969487, 4873150928, 4820094144, 3338263310, 4820211571, 400426688, 5482056535, 4820027533, 4820075718, 4820029872, 4802479866, 4802886612, 4820060403, 4802885329, 4820369208, 4820263011, 5426762331, 4820241539, 4820882444, 4820139798, 3337945323, 4802141828, 5015756115, 3337839486, 4820022630, 4820317884, 294847235, 4802911019, 7729330, 4802086681, 4820213881, 1268612, 4820182428, 4820060273, 765796, 336026206, 3630655861, 4802474017, 4820131087, 4820249664, 17504593, 3337977400, 4820275344, 4820159452, 4820155939, 4802145976, 3337873957, 4820011337, 4820139892, 4820235553, 4802478945, 4820150166, 4820137231, 4820027768, 5015675434, 4820097093, 4820106854, 4802475937, 4820238636, 4802140024, 4820102555, 4873178313, 4820092284, 4820017068, 4820108984, 4820093712, 4820414802, 4802125400, 4820248123, 4820134821, 3631267648, 5015611256, 4820072969, 3338285984, 5777359786, 4820020573, 4820221821, 4820032326, 4820339572, 4802500501, 4820029939, 4820309432, 4820365604, 4820077588, 5015910716, 4820099283, 3631103057, 5777397608, 3996144384, 4820137594, 3337941253, 717307, 4817689282, 4802150882, 4820234841, 4820248458, 4820075906, 4055309335, 4820140159, 4820800769, 4820184070, 4802475358, 4802252539, 4820182804, 4819956898, 4825181162, 4820096878, 4820309577, 4820076204, 4820265848, 4802892317, 4820027949, 4820028611, 4820013686, 4820030571, 208018577, 4820234970, 662219, 4820211335, 4820023833, 4820031077, 4802885948, 4820108912, 4820136784, 4802863302, 4820097855, 4820221501, 4820279445, 1391018274, 4802888720, 4820254605, 4868433228, 3311456331, 4820176926, 379051226, 4820093820, 4820361048, 4874238388, 4820257768, 4802126722, 4820025514, 4802894130, 4820115846, 4820348032, 4820091879, 4820138803, 5119058619, 4820315682, 4820239047, 5017329882, 4820019870, 4802087267, 2159964912, 5015784997, 4802864522, 4802467031, 4820359145, 610492276, 4820003318, 5116299323, 3631068861, 4820104884, 4820137454, 4802888471, 4820239515, 4828868239, 4820395564, 1391014532, 4820216965, 4823596976, 4820076013, 4820028497, 4820108789, 4820023224, 4825194215, 4820059312, 1094276, 3725231375, 4868544361, 4802501726, 4820268324, 4802850526, 4802517408, 4820097460, 4820110435, 4820168389, 4820890825, 2903927854, 4820073834, 4820238799, 4820072284, 4820103514, 4820133029, 4820251732, 4820200330, 4820058940, 4820025418, 6087718021, 4802905484, 4820075950, 5015768050, 4820094644, 4802042730, 4820060839, 4820318086, 4820250966, 4873940397, 5656207113, 4820134393, 4820274279, 4820314713, 4802093375, 4820027743, 4820095225, 4820060364, 4820182505, 4820186361, 1390865008, 4802889491, 4875327087, 4820028588, 4802478358, 4819984634, 4820110705, 4820137642, 3453181610, 4820211120, 4892979988, 4802131154, 4820508012, 4802092023, 5128031787, 1390835522, 4820103815, 4820180720, 5111809840, 4156962711, 4820391160, 4820096646, 4802096983, 4820270069, 4820106413, 4820109693, 3338247095, 3682630441, 4820110438, 4820076342, 4820249376, 5015767025, 4820140214, 4820288180, 4820010836, 4820110704, 4820059898, 4820434724, 4820018672, 4820235929, 97965176, 3337863134, 4802476235, 4820095816, 3337943252, 4820139626, 5990969054, 4820261784, 3337943308, 5361768622, 4820157251, 4820461702, 4820180815, 4820092162, 4820095917, 4820076688, 3338000908, 4820110605, 4820058959, 4820149848, 4820020431, 4820183561, 4820429618, 4820237646, 4820156756, 336052738, 4802863311, 4842134203, 4820238819, 4820250967, 5015839737, 5017353096, 4802518166, 4820137595, 4820029853, 4802473051, 4820262670, 4820793366, 4874102959, 4820131656, 4820094660, 4802803369, 4820318859, 3251994396, 4820060534, 4820218016, 4820315033, 4820338068, 4820211498, 4802122506, 4802904854, 4820072882, 3338292900, 4820099282, 3454211177, 4820201016, 4820059241, 5115314188, 1390856647, 5015668309, 3083147088, 4820228101, 38659515, 4820092639, 4820238618, 4820201363, 4820105141, 4820175223, 1390736743, 4820095284, 1436372991, 4820097459, 4820220191, 4820110578, 4820144505, 2358222040, 4820417050, 4802909509, 4820135318, 4820094952, 4820102717, 5015624394, 4819998329, 3375741953, 4802863642, 4821144149, 3739434716, 4820310542, 4802121986, 4820223869, 4820358128, 4820235086, 4820354448, 4820096364, 4820155831, 4820155901, 4802885672, 4820199831, 449612510, 4802526614, 3631262430, 4820422455, 4819968244, 3338203067, 4357780704, 4820368432, 3631404893, 4802524391, 4821123539, 4820073776, 4820059785, 4868526569, 4819966895, 4820234878, 4820108985, 4820076494, 4820072868, 4802913627, 1390870425, 3337967698, 4820310690, 4820320313, 4820240809, 4802129608, 4802885186]

def filter_for_stats(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    m_id = int(splits[0].strip())
    if m_id in negatives_ids:
        return True
    else:
        return False
"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-seventh/*", use_unicode=False)
stats = distFile.filter(filter_for_stats)
"""

"""
squeeze them in!

"""

def filter_for_other(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    m_id = int(splits[0].strip())
    if m_id in negatives_ids:
        return False
    else:
        return True

def filter_for_squeeze_cpu(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    cpu = float(splits[4].strip())
    if cpu < 43.77:
        return 43.77 - cpu
    else:
        return 0

def filter_for_squeeze_mem(line):
    splits = line.replace("\"","").replace("(", "").replace(")", "").replace("\'","").split(",")    
    mem = float(splits[5].strip())
    if mem < 84.06:
        return 84.06 - mem
    else:
        return 0

distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-sixth-day-*/*", use_unicode=False)
total_spare_cpu = distFile.filter(filter_for_other).map(filter_for_squeeze_cpu).reduce(add)
total_spare_mem = distFile.filter(filter_for_other).map(filter_for_squeeze_mem).reduce(add)
total_distance = distFile.filter(filter_for_other).map(total_distance).reduce(add)
total_up = distFile.filter(filter_for_other).map(total_uptime).reduce(add)


"""
New configuration suggestion

  $float[ ] totalCost;$
  $float totalCpu = 0;$
  $float totalMemory = 0;$

\ForAll{machine in machinesFromReport} {
    machine.cpu += totalCpu;
    machine.memory += totalMemory;
}

\ForAll{scheme in availableSchemes} {
    hoursPerDay = scheme.getHours();
    cpuCap = hoursPerDay * clusterCpuAvgPerHour;
    memCap = hoursPerDay * memoryCpuAvgPerHour;

    vmsReqCpu = totalCpu / cpuCap;
    vmsReqMem = totalMemory / memCap;

    totalCost[scheme] = min(vmsReqCpu, vmsReqMem) * scheme.cph;
}

\Return{min(totalCost)}

"""
distFile = sc.textFile("/Users/ksmuga/workspace/data/out/transformation-sixth-day-*/*", use_unicode=False)
distFile.filter(filter_for_stats).map(total_cpu)
distFile.filter(filter_for_stats).map(total_mem)



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
