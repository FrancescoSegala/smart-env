import json
import time
import datetime
import random


default_values_s = {"air":5.0 , "temp":20.0 , "light":1.6 }
floating_s = 1.0
min_time_w = 1
max_time_w = 6

############################### Sensor Class ###################################
class Sensor:
    'the count of all the sensors present in the room'
    all_sensors = 0
    sensors_level = {}

    def __init__(self, type, location):
        self.type = type
        self.location = location
        self.id = type+location+str(Sensor.all_sensors)
        Sensor.sensors_level[self.id] = default_values_s[type]
        Sensor.all_sensors+=1
        print("Created Sensor "+self.id)


    def get_value(self):
        #this will update the value for the current sensor
        incr = floating_s
        if self.type == "light":
            incr = 0.2
        value = random.uniform(Sensor.sensors_level[self.id]-incr , Sensor.sensors_level[self.id]+ incr )
        if value < 0 and self.type == "light":
            value += 0.2
        Sensor.sensors_level[self.id] = value
        return value

    def push_value(self):
        #this pack and push the message and then pasue for a random time
        ts = datetime.datetime.now().timestamp()
        message = {'Type':self.type, 'Value':self.get_value(), 'Timestamp':ts, 'Id':self.id, 'Location':  self.location}
        message=json.dumps(message)
        #wait some seconds before send another value
        time.sleep(random.randint(min_time_w , max_time_w ))
        print(message)
        return message


################################################################################
