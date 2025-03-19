import json
from confluent_kafka import Producer
import time

p = Producer({'bootstrap.servers': 'localhost:29092,localhost:39092,localhost:49092'})
topic_name='my_topic'

for i in range(10):
    message={'this is message # :':i}
    serialized_message = json.dumps(message).encode('utf-8')
    p.produce(topic_name,serialized_message)
    print(f'message sent:{message}')
    time.sleep(0.5)

p.flush()
