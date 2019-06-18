from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
producer.produce(
    'testxyz',
    '{"hello": "world"}'
)
print(producer.poll(60))
print('..produced.')
producer.produce(
    'testabc',
    '{"hello": "world"}'
)
print(producer.poll(60))
print('..produced.')
