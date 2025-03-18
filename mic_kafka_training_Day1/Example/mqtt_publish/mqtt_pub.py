import paho.mqtt.client as mqtt
import time
import json
import dotenv
import os

class MQTTPublish:
    def __init__(self):
        dotenv.load_dotenv()
        self.mqtt_broker = os.environ["MQTT_BROKER"]
        self.mqtt_port = int(os.environ["MQTT_PORT"])
        self.client_id = "mqtt_publisher_oat"
        self.client = mqtt.Client(self.client_id)
        self.client.on_connect = self.on_connect
        self.topic_name = os.environ["MQTT_PUB_TOPIC1"]

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            for topic in self.sub_topics:
                client.subscribe(topic.strip())
        else:
            print(f"Failed to connect, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        print("Disconnected from MQTT Broker")

    def publish_messages(self,topic_num,delay_time,msg_num,pub_loop,init):
        message_data={}
        for i in range(init,pub_loop+init):
            for j in range(1, topic_num+1):  
                topic = f"{self.topic_name}/no_{j}"
                for k in range(1,msg_num+1):
                    key = f"data{k}"
                    message_data[key] = i
                self.client.publish(topic, json.dumps(message_data))
            time.sleep(delay_time)  

    def run(self):
        self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
        self.publish_messages(10,0.001,20,10,1)

if __name__ == "__main__":
    publisher = MQTTPublish()
    publisher.run()