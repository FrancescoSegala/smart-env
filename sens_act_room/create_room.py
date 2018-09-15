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
#TODO #1 names refactoring and fix this _default_path not needed here! just move some params
_default_path = "actuators_room.txt"
#TODO #2 embedd the number of sensors and the number of actuators elsewhere


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

__n_sensors_default = 10
__n_actuators_defalut = 5

actual_actuators_level = {}

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



def mock_changes(sensors_list , actuators_list):
    #this method fakes the environment, so if an actuator is set it will influence the value given by the sensors
    prev_a = {"air":actual_actuators_level["air"],"temp":actual_actuators_level["temp"],"light":actual_actuators_level["light"]}
    #take the actual value before the actuators_ change
    for type in types_list:
        for actuator in actuators_list:
            #if an actuator change a value then update the sensors related to that type
            if  actuator.type == type and Actuator.actuators_level[actuator.id] != prev_a[type] :
                prev_a[type] = Actuator.actuators_level[actuator.id]
                for sensor in sensors_list:
                    if sensor.type == type :
                        Sensor.sensors_level[sensor.id] += ( Sensor.sensors_level[sensor.id] - Actuator.actuators_level[actuator.id] )


def get_sensors_list(n_sensors , location ):
    sensors_list=[0]*n_sensors
    for i in range(0,n_sensors):
        type=types_list[random.randint(0,n_sensors)%3]
        sensors_list[i] = Sensor(type, location)
    return sensors_list


def get_actuators_list(n_actuators , location ):
    actuators_list=[0]*n_actuators
    for k in range(0,n_actuators):
        type = types_list[random.randint(0,n_actuators)%3]
        actuator = Actuator(type, location)
        actuators_list[k] = actuator
        actual_actuators_level[actuator.type] = actuator.get_value()
    return actuators_list

def start_room(location, producer, actuators_filename = _default_path,
                                    n_sensors = __n_sensors_default , n_actuators = __n_actuators_defalut ):
    open( actuators_filename , 'w')
    sensors_list = get_sensors_list( n_sensors , location )
    actuators_list = get_actuators_list( n_actuators , location )
    j = 0
    l = 0
    while True:
        producer.send_message( sensors_list[j].push_value() )
        actuators_list[l].get_input()
        j += 1
        l += 1
        j = j % n_sensors
        l = l % n_actuators
        if l == 0
            #perform the changes periodically every time all the actuators have been scanned 
            mock_changes(sensors_list, actuators_list)


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
    n_sensors=__n_sensors_default
    n_actuators=__n_actuators_defalut
    address=""
    actuators_filename= _default_path
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
    start_room( location, producer, actuators_filename, n_sensors, n_actuators)


if __name__ == "__main__":
    main()
