from kafka import KafkaConsumer


_kafka_topic = "test"
_kafka_broker_adrress = 'localhost:9092'

 #By providing auto_offset_reset='earliest' you are telling Kafka to return messages from the beginning.
 #The parameter consumer_timeout_ms helps the consumer to disconnect after the certain period of time.
 #Once disconnected, you can close the consumer stream by calling consumer.close()

#this is the basic way to retriee messages from a remote kafka broker
consumer = KafkaConsumer(_kafka_topic,auto_offset_reset='earliest',bootstrap_servers=_kafka_broker_adrress)
for msg in consumer:
    print ("topic: "+msg.topic+" messaggio: "+msg.value.decode("utf-8") )

#consumer.close()
