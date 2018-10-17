import json
import random
import hashlib
import requests
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from kafka import KafkaProducer
from sensors import *
from actuators import *

################################################################################
'''
main method has to be launched when a new room is instantiated, so for each room there is a generated stream

    usage : python3 create_room.py  -l #address [ [-s #sens -a #act]  || -n #room-name]

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


types_list = ["air","temp","light"]
kappa = {}

x_lock = threading.Lock()

#those ADT keeps record of the sensors and actuators active in the room
room_sensors = {}
room_actuators = {}

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

# HTTPRequestHandler class
class actuatorHTTPrequestHandler(BaseHTTPRequestHandler):

    #POST
    def do_POST(self):
        #path of the form: host/:user_id/:env_id
        # Doesn't do anything with posted data
        self.send_response(200)
        # Send headers
        self.send_header('Content-type','text/html')
        self.end_headers()
        path = self.path.split("/")
        if len(path) < 3 :
            self.wfile.write(bytes("invalid format", "utf8"))
            return
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
        message = start_room( location, producer,user_id,env_id, n_sensors, n_actuators)
        self.wfile.write(bytes(message, "utf8"))
        return


    # GET
    def do_GET(self):
        #path of the form: host/:id/:value
        # Send response status code
        self.send_response(200)
        # Send headers
        self.send_header('Content-type','text/html')
        self.end_headers()
        path = self.path.split("/")
        if len(path) < 3 :
            self.wfile.write(bytes("invalid format", "utf8"))
            return
        id = path[1]
        value = path[2]
        if id not in list(room_actuators.keys()):
            print(list(room_actuators.keys()))
            self.wfile.write(bytes("invalid id", "utf8"))
            return
        message = "value Updated"
        if update_actuator(id, value):
            print("actuator ", id, " value changed to ", value)
            # Send message back to client
        else :
            message = "cannot update id "+ id
        # Write content as utf-8 data
        self.wfile.write(bytes(message, "utf8"))
        return



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
    while True:
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


def env_bind_endpoints_worker(user_id,env_id):
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
    post_url = "http://"+config["RAILS_HOST"]+"/envs/"+env_id+"/endpoints?user_id="+user_id
    r = requests.post(post_url, json=data )
    print("Binding done")
    return


def send_sensor_data_worker( producer):
    sensor_ids = list(room_sensors.keys())
    n_sensors = len(sensor_ids)
    i=0
    while True:
        with x_lock:
            producer.send_message( room_sensors[sensor_ids[i]].push_value() )
            #print ("sensor ", sensor_ids[i], "sent his data")
        #wait some random time before send another value
        time.sleep(random.randint(min_time_w , max_time_w ))
        i+=1
        i = i % n_sensors


def http_actuator_server_worker(host, port):
  print('starting http actuator server...')
  # Choose port 8080, for port 80, which is normally used for a http server, you need root access
  server_address = (host, port)
  httpd = HTTPServer(server_address, actuatorHTTPrequestHandler)
  print('running  http actuator server...')
  httpd.serve_forever()


def statrt_room_http( http_server_host ):
    host = http_server_host.split(":")
    hostname = host[0]
    port = int(host[1])
    actuator_thread = threading.Thread(target=http_actuator_server_worker, args=(hostname,port,))
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
def start_room(location, producer , user_id, env_id, n_sensors = __n_sensors_default , n_actuators = __n_actuators_defalut ):
    init_kappa()
    get_sensors_list( n_sensors , location )
    get_actuators_list( n_actuators , location )
    actuator_ids = list(room_actuators.keys())
    sensor_thread = threading.Thread(target=send_sensor_data_worker, args=(producer,))
    sensor_thread.start()
    mock_changes_thread = threading.Thread(target=mock_changes_worker, args=(actuator_ids,))
    mock_changes_thread.start()
    bind_endpoints_thread = threading.Thread(target=env_bind_endpoints_worker ,args=(user_id,env_id,))
    bind_endpoints_thread.start()
    return "Room Starting..."
    #sensor_thread.join()
    #mock_changes_thread.join()

################################################################################





def print_usage():
    print("\nusage: python3 create_room.py  -l #address [ [-s #sens -a #act] -n or --name #room-name]\n")


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
    #take config params
    with open(_config_file_json) as f:
        config = json.load(f)
    _room_name = config["ROOM_NAME"]
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
    print("Room "+_room_name+" created!")
    with open(_config_file_json, "r") as jsonFile:
        data = json.load(jsonFile)

    data["LOCATION"] = get_location( address )
    data["NSENSOR"] = n_sensors
    data["NACTUATORS"] = n_actuators
    with open(_config_file_json, "w") as jsonFile:
        json.dump(data, jsonFile)

    http_server_host = config["HTTP_SERVER_HOST"]
    statrt_room_http(http_server_host)

if __name__ == "__main__":
    main()
