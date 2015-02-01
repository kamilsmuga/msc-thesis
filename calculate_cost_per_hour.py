#!/usr/bin/env python

import matplotlib.pyplot as plot

lights = []
mediums = []
heavies = []
entities = []

class Entity:
    def __init__(self, name, light, medium, heavy):
        self.name = name
        self.light = light
        self.medium = medium
        self.heavy = heavy

    def calc_cost_per_hour(self, mode, hours_per_day):
        upfront = int(mode["upfront"])
        cost_per_hour = float(mode["perhour"])
        cph = (upfront + (365 * cost_per_hour * hours_per_day)) / (365 * hours_per_day)
        return cph   

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

c_entity = Entity(c_name, c_light, c_medium, c_heavy)
m_entity = Entity(m_name, m_light, m_medium, m_heavy)
h_entity = Entity(h_name, h_light, h_medium, h_heavy)

entities.extend([c_entity, m_entity, h_entity])

for e in entities:
    lights = [ e.calc_cost_per_hour(e.light, x) for x in range(1,25) ] 
    mediums = [ e.calc_cost_per_hour(e.medium, x) for x in range(1,25) ]
    heavies = [ e.calc_cost_per_hour(e.heavy, x) for x in range(1,25) ]

    t = range(24)
    plot.plot(t, lights, t, mediums, t, heavies)
    plot.savefig(e.name + '.png')
    plot.clf()



#    print "light,medium,heavy"
#    for i in range(24):
#        print "%s,%s,%s" % (lights[i], mediums[i], heavies[i])