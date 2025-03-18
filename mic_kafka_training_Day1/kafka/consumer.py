from confluent_kafka import Consumer


c = Consumer({
    'bootstrap.servers': '192.168.0.113:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})

c.subscribe(['k_datamicdemo1'])# Topic name in kafka

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))