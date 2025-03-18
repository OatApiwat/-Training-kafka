import paho.mqtt.client as mqtt
import os
import dotenv
import json
from confluent_kafka import Producer

class MQTTSubscriber:
    def __init__(self):
        dotenv.load_dotenv()
        self.mqtt_broker = os.environ["MQTT_BROKER"]
        self.mqtt_port = int(os.environ["MQTT_PORT"])
        self.sub_topics = os.environ["MQTT_SUB_TOPIC"].split(",")
        self.client_id = "mqtt_subscriber_oat"
        self.client = mqtt.Client(self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.kafka_server = os.environ["KAFKA_SERVER"]
        self.msg_count = 0
        self.producer = Producer({
            'bootstrap.servers': self.kafka_server,
            # ทำให้สามารถส่งข้อมูลได้ครบมากขึ้น เช่นถ้ามี data 10k ตัวอาจจะส่งไม่ครบ
            'acks': 'all',  
            'batch.size': 16384, # 16KB
            # 'batch.size':
            'linger.ms': 10,  
        })

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            for topic in self.sub_topics:
                client.subscribe(topic.strip())
        else:
            print(f"Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        print("Disconnected from MQTT Broker")

    def on_message(self, client, userdata, msg):
        msg_topic_type_sp = (msg.topic).split("/") #data/mic/demo1/a1 ==> data mic demo a1
        if len(msg_topic_type_sp) >= 3:
            msg_topic_type = msg_topic_type_sp[0] + msg_topic_type_sp[1] + msg_topic_type_sp[2] #datamicdemo1
            k_topic = f'k_{msg_topic_type}' 
            k_msg_payload = json.loads(msg.payload.decode())
            k_msg = {"topic": str(msg.topic), **k_msg_payload}
            self.kafka_producer(k_topic,k_msg)
            print(f"Received message: {k_topic} -> {k_msg}")

    def kafka_producer(self,k_topic,k_msg):
        message = k_msg
        message = json.dumps(message).encode('utf-8')
        self.producer.produce(topic=k_topic,key=k_topic,value=message)
        self.producer.flush()
        print("sent data")

    def run(self):
        self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
        self.client.loop_forever()

if __name__ == "__main__":
    subscriber = MQTTSubscriber()
    subscriber.run()
