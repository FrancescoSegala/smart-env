import json
import random
import hashlib
import requests
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from kafka import KafkaProducer
from kafka import KafkaConsumer
from sensors import *
from actuators import *

################################################################################
'''
main method has to be launched when a new room is instantiated, so for each room there is a generated stream

    usage : usage: python3 create_room.py  -l #address -n or --name #name[ [-s #sens -a #act] ]

'''

#NOTE #1
#the method create_room start some sensors and actuators and push the sensor value to the kafka broker registered in _config_file_json

#NOTE #2
#this module is almost self-contained, whenever a real env is available one should only write the connection to a kafka broker or message queue part.


#NOTE #3
#this module should start before the biding client-side of the environment, otherwise the binding function will fail

######################### CONSTANTS & ATD ######################################

google_maps_url = "https://maps.googleapis.com/maps/api/geocode/json?address="
_config_file_json = "config.json"
binding_json = "binding.json"

__n_sensors_default = 10
__n_actuators_defalut = 5
min_time_w = 1
max_time_w = 6
minute = 60
_RUN_THREADS = True


types_list = ["air","temp","light"]
kappa = {}

x_lock = threading.Lock()

#those ADT keeps record of the sensors and actuators active in the room
room_sensors = {}
room_actuators = {}

############################### Kafka Producer/Consumer Class ##################

class RoomConsumer:
    room_started = False

    def __init__(self, _address, _topic, _room_name):
        self._kafka_topic = _topic
        self._room_name = _room_name
        self._kafka_broker_adrress = _address
        self.consumer = KafkaConsumer(bootstrap_servers=[_address])
        self.consumer.subscribe([_topic])

    def recv_messages(self):
        for msg in self.consumer:
            self.parse_message( msg.value )

    def parse_message(self,message):
        global _RUN_THREADS
        message_d = message.decode("utf-8")
        if message_d == "STOP_KAFKA_CONSUMER":
            print("SHUTDOWN KABOOM ")
            _RUN_THREADS = False
            exit(1)
            return
        path = message_d.split("/")
        if len(path) < 3 :
            print("path format not valid")
            return
        if path[0] == "start_room" and not RoomConsumer.room_started:
            user_id = path[1]
            env_id = path[2]
            #create the room stuff!!
            with open(_config_file_json) as f:
                config = json.load(f)
            TOPIC_NAME_S = env_id
            n_sensors = config["NSENSOR"]
            n_actuators = config["NACTUATORS"]
            location = config["LOCATION"]
            producer = RoomProducer(config["BROKER_HOST"],TOPIC_NAME_S)
            return_message = start_room( location,self._room_name, producer,user_id,env_id, n_sensors, n_actuators)
            RoomConsumer.room_started = True
            print(return_message)
            return
        if path[0] == "change_value":
            id = path[1]
            value = path[2]
            if id not in list(room_actuators.keys()):
                print(list(room_actuators.keys()))
                print("invalid id")
                return
            return_message = "value Updated"
            if update_actuator(id, value):
                print("actuator ", id, " value changed to ", value)
                # Send message back to client
            else :
                return_message = "cannot update id "+ id
            print(return_message)
            return





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
    #response = requests.get(google_maps_url+address)
    #resp_json_payload = response.json()
    #print(resp_json_payload)
    #return resp_json_payload["results"][0]["place_id"]
    return "_"+address.rstrip().replace(" ", "_")


#NOTE
#in order to use google maps API over the limit of usage you need to register your billing address [I have not RN]
# when i am able to get one just uncomment the get_location() function



def mock_changes_worker(actuators_id_list):
    #this method fakes the environment, so if an actuator is set it will influence the value given by the sensors
    prev = {}
    #sleep to wait for the room to setup
    time.sleep( minute )
    for id in actuators_id_list:
        with x_lock:
            prev[id] = room_actuators[id].get_value()
    while _RUN_THREADS:
        print( "checking room changes..." )
        for id in actuators_id_list:
            with x_lock:
                curr_value = room_actuators[id].get_value()
                type = room_actuators[id].type
            if curr_value != prev[id]:
                #change in sensor value for the type
                for sensor in list(room_sensors.values()):
                    if sensor.type == type :
                        var = [-1,1]
                        r = var[random.randint(0,1)]
                        with x_lock:
                            Sensor.sensors_level[sensor.id] += r*abs( Sensor.sensors_level[sensor.id] - curr_value )
                        print("Env condition changed")
                prev[id] = curr_value
        time.sleep( 2 * minute )




def get_sensors_list(n_sensors , location ):
    for i in range(0,n_sensors):
        type=types_list[random.randint(0,n_sensors)% len(types_list) ]
        sensor = Sensor(type, location)
        room_sensors[sensor.id] = sensor


def init_kappa():
    with open(_config_file_json) as f:
        config = json.load(f)
    for k in config["KAPPA_VALUES"]:
        kappa[k.lower()] = config["KAPPA_VALUES"][k]


def get_actuators_list(n_actuators , location ):
    for k in range(0,n_actuators):
        type = types_list[random.randint(0,n_actuators)% len(types_list)]
        actuator = Actuator(type, location, kappa[type])
        room_actuators[actuator.id] = actuator


def update_actuator(id, value):
    with x_lock:
        room_actuators[id].set_value(float(value))
    return True


def env_bind_endpoints_worker(user_id,env_id,_room_name,location):
    with open(_config_file_json) as f:
        config = json.load(f)
    with open(binding_json, "r") as jsonFile:
        data = json.load(jsonFile)
    actuator_ids = list(room_actuators.keys())
    sensor_ids = list(room_sensors.keys())
    for actuator in actuator_ids:
        data["Endpoints"]["actuators"][actuator] = {"type":room_actuators[actuator].type, "location":room_actuators[actuator].location, "kappa":room_actuators[actuator].kappa}
    for sensor in sensor_ids:
        data["Endpoints"]["sensors"][sensor] = {"type":room_sensors[sensor].type, "location":room_sensors[sensor].location}
    data["Topic"] = env_id
    data["Actuator_topic"] = _room_name+location
    post_url = "http://"+config["RAILS_HOST"]+"/envs/"+env_id+"/endpoints?user_id="+user_id
    r = requests.post(post_url, json=data )
    print("Binding done")
    return


def send_sensor_data_worker( producer):
    sensor_ids = list(room_sensors.keys())
    n_sensors = len(sensor_ids)
    i=0
    while _RUN_THREADS:
        with x_lock:
            producer.send_message( room_sensors[sensor_ids[i]].push_value() )
            #print ("sensor ", sensor_ids[i], "sent his data")
        #wait some random time before send another value
        time.sleep(random.randint(min_time_w , max_time_w ))
        i+=1
        i = i % n_sensors


def kafka_actuator_server_worker(_room_name, location):
    topic_name = _room_name+location
    with open(_config_file_json) as f:
        config = json.load(f)
    consumer = RoomConsumer(config["BROKER_HOST"],topic_name, _room_name)
    consumer.recv_messages()


def start_room_receiver( _room_name, location ):
    actuator_thread = threading.Thread(target=kafka_actuator_server_worker, args=(_room_name, location,))
    actuator_thread.start()
    actuator_thread.join()


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
        else :
            return "Room Already started"
    wrapper.has_run = False
    return wrapper


@run_once
def start_room(location,_room_name, producer , user_id, env_id, n_sensors = __n_sensors_default , n_actuators = __n_actuators_defalut ):
    init_kappa()
    get_sensors_list( n_sensors , location )
    get_actuators_list( n_actuators , location )
    actuator_ids = list(room_actuators.keys())
    sensor_thread = threading.Thread(target=send_sensor_data_worker, args=(producer,))
    sensor_thread.start()
    mock_changes_thread = threading.Thread(target=mock_changes_worker, args=(actuator_ids,))
    mock_changes_thread.start()
    bind_endpoints_thread = threading.Thread(target=env_bind_endpoints_worker ,args=(user_id,env_id,_room_name,location,))
    bind_endpoints_thread.start()
    return "Room Starting..."
    #sensor_thread.join()
    #mock_changes_thread.join()

################################################################################



def print_usage():
    print("\nusage: python3 create_room.py  -l #address -n or --name #name[ [-s #sens -a #act] ]\n")


def is_int(value):
    try:
        value = int(value)
        return True
    except ValueError:
        return False


def main():
    #command line parsing
    if "-l" not in sys.argv and ("-n" not in sys.argv or "--name" not in sys.argv) :
        print_usage()
        return
    n_sensors=__n_sensors_default
    n_actuators=__n_actuators_defalut
    address=""
    #take config params
    with open(_config_file_json) as f:
        config = json.load(f)
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
        if param == "-n" or param == "--name":
            _room_name =sys.argv[i+1]
    #start program
    with open(_config_file_json, "r") as jsonFile:
        data = json.load(jsonFile)

    location = get_location( address )

    data["LOCATION"] = location
    data["NSENSOR"] = n_sensors
    data["NACTUATORS"] = n_actuators
    with open(_config_file_json, "w") as jsonFile:
        json.dump(data, jsonFile)

    print("Room "+_room_name+" created!")
    print("Topic "+_room_name+location+" created, waiting...")
    start_room_receiver(_room_name,location)

if __name__ == "__main__":
    main()
