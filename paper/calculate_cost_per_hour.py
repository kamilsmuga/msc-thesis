#!/usr/bin/env python

lights = []
mediums = []
heavies = []

# values for size: cc2.8xlarge region: us-east
# 1 year reservation
light = { "upfront" : "1762", "perhour" : "0.904" }
medium = { "upfront" : "4146", "perhour" : "0.54" }
heavy = { "upfront" : "5000", "perhour" : "0.361" }

def calc_cost_per_hour(upfront, cost_per_hour, hours_per_day):
    upfront = int(upfront)
    cost_per_hour = float(cost_per_hour)
    cph = (upfront + (365 * cost_per_hour * hours_per_day)) / (365 * hours_per_day)
    return cph

lights = [calc_cost_per_hour(light["upfront"], light["perhour"], x) for x in range(1,25)] 
mediums = [calc_cost_per_hour(medium["upfront"], medium["perhour"], x) for x in range(1,25)] 
heavies = [calc_cost_per_hour(heavy["upfront"], heavy["perhour"], x) for x in range(1,25)] 

print "light,medium,heavy"
for i in range(24):
    print "%s,%s,%s" % (lights[i], mediums[i], heavies[i])

