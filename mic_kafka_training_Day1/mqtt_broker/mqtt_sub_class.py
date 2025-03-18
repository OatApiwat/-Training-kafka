import paho.mqtt.client as mqtt
import os
import dotenv

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
        self.message_count = 0 

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            for topic in self.sub_topics:
                client.subscribe(topic.strip())
        else:
            print(f"Failed to connect, return code {rc}")

    def on_message(self, client, userdata, msg):
        self.message_count += 1 
        print(self.message_count)
        print(f"Received message: {msg.topic} -> {msg.payload.decode()}")

    def on_disconnect(self, client, userdata, rc):
        print("Disconnected from MQTT Broker")

    def run(self):
        self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
        self.client.loop_forever()

if __name__ == "__main__":
    subscriber = MQTTSubscriber()
    subscriber.run()
