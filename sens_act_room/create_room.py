import json
import random
import hashlib
import requests
import sys
from kafka import KafkaProducer
from sensors import *
from actuators import *


################################################################################
'''
main method has to be launched when a new room is instantiated, so for each room there is a generated stream

    usage : python3 create_room.py  -l #address [ [-s #sens -a #act]  -p || --path #actuator-filename --name || -n #room-name]

'''
#TODO names refactoring and fix this _default_path not needed here! just move some params
_default_path = "actuators_room.txt"

#NOTE #1
#this module create a file [in a hardwired path or a custom one] where there are couple k:V where k=[Actuator ID]  and v=[Actuator current value]
#one on each line this because the value of the actuator shoul be bounded to the room created, is not advised to push the value "far" in a server
#it will produce the same result, however this a mock for real sensors and in real application this value is bounded inside the actuator so close

#NOTE #2
#the method create_room start some sensors and actuators

#NOTE #3
#this module is almost self-contained, whenever a real env is available one should only write the connection to a kafka broker or message queue part.

#########################CONSTANTS##############################################

google_maps_url = "https://maps.googleapis.com/maps/api/geocode/json?address="
_config_file_json = "config.json"
min_time_w = 1
max_time_w = 6
__air_default_sensor = 2.0
__temp_default_sensor = 2.0
__light_default_sensor = 2.0
__air_default_actuator = 2.0
__temp_default_actuator = 2.0
__light_default_actuator = 2.0
__n_sensors_default = 10
__n_actuators_defalut = 5
floating_s = 1.0

############################### used ADT #######################################

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


def get_sensors_list(n_sensors ):
    sensors_list=[0]*n_sensors
    for i in range(0,n_sensors):
        type=types_list[random.randint(0,n_sensors)%3]
        sensors_list[i] = Sensor(type, location)
    return sensors_list


def get_actuators_list(n_actuators ):
    actuators_list=[0]*n_actuators
    for k in range(0,n_actuators):
        type = types_list[random.randint(0,n_actuators)%3]
        actuators_list[k] = Actuator(type, location, actuators_filename)
    return actuators_list

def start_room(location, producer, actuators_filename = _default_path,
                                    n_sensors = __n_sensors_default , n_actuators = __n_actuators_defalut ):
    open( actuators_filename , 'w')
    sensors_list = get_sensors_list( n_sensors )
    actuators_list = get_actuators_list( n_actuators )
    j = 0
    l = 0
    while True:
        producer.send_message( sensors_list[j].push_value() )
        #TODO
        return

        actuators_list[l].get_input()
        j += 1
        l += 1
        j = j % n_sensors
        l = l % n_actuators
        mock_changes()


################################################################################


def print_usage():
    print("\nusage: python3 create_room.py  -l #address [ [-s #sens -a #act] --path or -p #actuator-filename -n or --name #room-name]\n")


def is_int(value):
    try:
        value = int(value)
        return True
    except ValueError:
        return False


def main():
    #command line parsing
    if "-l" not in sys.argv:
        print_usage()
        return
    n_sensors=None
    n_actuators=None
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
        if param == "-a":
            if "-s" in sys.argv:
                if is_int(sys.argv[i+1]):
                    n_actuators=int(sys.argv[i+1])
                else :
                    print_usage()
                    return
            else :
                print_usage()
                print("NOTE: cannot be indicated the actuators and not the sensors")
                return
        if param == "-l" :
            address=sys.argv[i+1]
        if param == "-p" or param=="--path":
            actuators_filename=sys.argv[i+1]
        if param == "-n" or param == "--name":
            _room_name =sys.argv[i+1]
    #take config params
    with open(_config_file_json) as f:
        config = json.load(f)
    #start program
    _room_name = config["ROOM_NAME"]
    print("Room "+_room_name+" created!")
    location = get_location( address )
    #default is "MyEnv"
    TOPIC_NAME_S = location+_room_name
    producer = RoomProducer(config["BROKER_HOST"],TOPIC_NAME_S)
    start_room( address, producer, actuators_filename, n_sensors, n_actuators)


if __name__ == "__main__":
    main()
