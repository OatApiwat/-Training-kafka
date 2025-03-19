import json
from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': '192.168.0.113:29092,192.168.0.113:39092,192.168.0.113:49092'})
# p = Producer({'bootstrap.servers': 'kafka-1:19092,kafka-2:19092,kafka-3:19092'})
topic_name='my_topic'

for i in range(2):
    message={'this is message # :':i}
    serialized_message = json.dumps(message).encode('utf-8')
    p.produce(topic_name,serialized_message)
    print(f'message sent:{message}')
    time.sleep(0.5)

p.flush()
