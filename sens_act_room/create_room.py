import json
import datetime
import random
import time
import hashlib
import requests
import os.path
import sys
from kafka import KafkaProducer



################################################################################

#TODO make the two classes thread safe, so they do not stuck the cpu

#NOTE #1
#this module create a file [in a hardwired path or a custom one] where there are couple k:V where k=[Actuator ID]  and v=[Actuator current value]
#one on each line this because the value of the actuator shoul be bounded to the room created, is not advised to push the value "far" in a server
#it will produce the same result, however this a mock for real sensors and in real application this value is bounded inside the actuator so close

#NOTE #2
#the method create_room start some sensors and actuators

#NOTE #3
#this module is almost self-contained, whenever a real env is available one should only write the connection to a kafka broker or message queue part.

#########################CONSTANTS##############################################
min_time_w = 1
max_time_w = 6
google_maps_url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
_default_path = "actuators_room.txt"
_room_name = ""
__air_default_sensor = 2.0
__temp_default_sensor = 2.0
__light_default_sensor = 2.0
__air_default_actuator = 2.0
__temp_default_actuator = 2.0
__light_default_actuator = 2.0
floating_s = 1.0

actual_sensors_level = {"air":__air_default_sensor,"temp":__temp_default_sensor,"light":__light_default_sensor}
actual_actuators_level = {"air":__air_default_actuator,"temp":__temp_default_actuator,"light":__light_default_actuator}
types_list = ["air","temp","light"]

############################### Kafka Producer Class ###########################

class RoomProducer:

    def __init__(self, _address, _topic):
        self._kafka_topic = _topic
        self._kafka_broker_adrress = _address
        self.producer = KafkaProducer(bootstrap_servers=[self._kafka_broker_adrress])

    def send_message(self, message):
        message_bytes = bytes(message, encoding='utf-8')
        self.producer.send(self._kafka_topic,value=message_bytes)
        self.producer.flush()



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
        time.sleep(random.randint(min_time_w , max_time_w ))
        print(message)
        return message


################################################################################
############################# Actuators Class ##################################

class Actuator:
        'the count of all the actuators present in the room'
        all_actuators = 0

        def __init__(self, type, location, actuators_filename=_default_path):
            self.type = type
            self.location = location
            self.id = type+location+str(Actuator.all_actuators)
            if os.path.isfile(actuators_filename):
                lines = [line.rstrip('\n') for line in open(actuators_filename)]
                for line in lines:
                    curr_id = line.split(":")[0]
                    if curr_id == self.id :
                        print("Actuator "+self.id+" alreay exist. ERROR")
                        del self
                        return
                with open(actuators_filename, "a") as myfile:
                    #TODO fix this the default value of the actuator
                    myfile.write(self.id+":2.0\n")
            print("Created Actuator "+self.id)
            Actuator.all_actuators+=1

        def __del__(self):
            class_name = self.__class__.__name__
            #print( class_name, "destroyed")

        def get_value(self):
            # this get the current value of the Actuator
            return actual_actuators_level[self.type]

        def set_value(self, value):
            # this set the acutator value to value
            actual_actuators_level[self.type] = value

        def get_input(self, actuators_filename=_default_path):
            # this get an external input : a value in a file in the form Actuator_ID:value
            #since the actuator number is limited it will not affect the performances
            if os.path.isfile(actuators_filename):
                lines = [line.rstrip('\n') for line in open(actuators_filename)]
                for line in lines:
                    curr_id,value = line.split(":")
                    if curr_id == self.id :
                        if value != actual_actuators_level[self.type]:
                            self.set_value(value)

################################################################################
######################### Room function ########################################

def get_location(address):
    response = requests.get(google_maps_url+address)
    resp_json_payload = response.json()
    return resp_json_payload["results"][0]["place_id"]



def mock_changes():
    #this method fakes the environment, so if an actuator is set it will influence the value given by the sensors
    prev_a = {"air":actual_actuators_level["air"],"temp":actual_actuators_level["temp"],"light":actual_actuators_level["light"]}
    for type in types_list:
        if actual_actuators_level[type] != prev_a[type]:
            actual_sensors_level[type] += ( actual_sensors_level[type] - actual_actuators_level[type] )
            prev_a[type] = actual_actuators_level[type]



def start_room(n_sensors, n_actuators, address, producer, actuators_filename = _default_path):
    open( actuators_filename , 'w')
    sensors_list=[0]*n_sensors
    actuators_list=[0]*n_actuators
    location = get_location( address )
    for i in range(0,n_sensors):
        type=types_list[random.randint(0,n_sensors)%3]
        sensors_list[i] = Sensor(type, location)
    for k in range(0,n_actuators):
        type = types_list[random.randint(0,n_actuators)%3]
        actuators_list[k] = Actuator(type, location, actuators_filename)
    j = 0
    l = 0
    while True:
        producer.send_message( sensors_list[j].push_value() )
        actuators_list[l].get_input()
        j += 1
        l += 1
        j = j % n_sensors
        l = l % n_actuators
        mock_changes()


################################################################################


def print_usage():
    print("\nusage: python3 create_room.py -s #sens -a #act -l #address [--path or -p #actuator-filename -n or --name #room-name]\n")


def is_int(value):
    try:
        value = int(value)
        return True
    except ValueError:
        return False


def main():
    #command line parsing
    if "-s" not in sys.argv or "-a" not in sys.argv or "-l" not in sys.argv:
        print_usage()
        return
    n_sensors=0
    n_actuators=0
    address=""
    actuators_filename=_default_path
    for i in range(1,len(sys.argv)):
        param = sys.argv[i]
        if param == "-s" :
            if is_int(sys.argv[i+1]) :
                n_sensors=int(sys.argv[i+1])
            else:
                print_usage()
                return
        if param == "-a" :
            if is_int(sys.argv[i+1]):
                n_actuators=int(sys.argv[i+1])
            else :
                print_usage()
                return
        if param == "-l" :
            address=sys.argv[i+1]
        if param == "-p" or param=="--path":
            actuators_filename=sys.argv[i+1]
        if param == "-n" or param == "--name":
            _room_name =sys.argv[i+1]
    #start program
    _room_name = str(n_sensors)+":"+str(n_actuators)+":"+address.replace(" ", "")
    print("Room "+_room_name+" created!")
    #TODO instead of test should be some meaningful name as room name but i can't rn create a topic from code
    producer = RoomProducer("localhost:9092","test")
    start_room(n_sensors, n_actuators, address, producer, actuators_filename)


if __name__ == "__main__":
    main()

'''
main method has to be launched when a new room is instantiated, so for each room there is a generate stream

    usage : python3 create_room.py -s #sens -a #act -l #address [-p || --path #actuator-filename --name || -n #room-name]

'''
