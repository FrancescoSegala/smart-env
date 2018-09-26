from kafka import KafkaProducer
import time

_kafka_topic = "10:3:viaraffaeleciasca60"
_kafka_broker_adrress = 'localhost:9092'
room_sensors_filename="./sens_act_room/sensors_room.txt"



class RoomProducer:

    def __init__(self, _address, _topic):
                _kafka_topic = _topic
                _kafka_broker_adrress = _address
                producer = KafkaProducer(bootstrap_servers=[_kafka_broker_adrress])


    def send_message(self, message, key_=""):
        message_bytes = bytes(message, encoding='utf-8')
        producer.send(_kafka_topic,key=key_,value=message_bytes)
        producer.flush()



producer = KafkaProducer(bootstrap_servers=[_kafka_broker_adrress])

producer.send(_kafka_topic,value=b"porca madonnina") #send is assynchronous
producer.flush() #this immediately flush the output

    #time.sleep(20)

    #to serialize the messages in any way just add the value before in the constructor
    #value_serializer=lambda v: json.dumps(v).encode('utf-8')
