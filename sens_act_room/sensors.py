import json
import time
import datetime


############################### Sensor Class ###################################
class Sensor:
    'the count of all the sensors present in the room'
    all_sensors = 0

    def __init__(self, type, location):
        self.type = type
        self.location = location
        self.id = type+location+str(Sensor.all_sensors)
        Sensor.all_sensors+=1
        print("Created Sensor "+self.id)

    def get_value(self):
        #this will update the value for the current sensor
        value = random.uniform(actual_sensors_level[self.type]-floating_s , actual_sensors_level[self.type]+ floating_s )
        actual_sensors_level[self.type] = value
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
