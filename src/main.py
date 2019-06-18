import json
from confluent_kafka import Consumer, KafkaError


def main():
    """Generator of kafka messages for a given set of topics."""
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'testgroup',
        'auto.offset.reset': 'earliest'
    })
    topics = ['testxyz', 'testabc']
    print('Subscribing to topics:', topics)
    consumer.subscribe(topics)
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of stream.')
                continue
            else:
                print('Error', msg.error())
        val = msg.value().decode('utf-8')
        try:
            data = json.loads(val)
            print('message', data)
        except ValueError as err:
            print(f'JSON parsing error: {err}')
            print(f'Message content: {val}')
    consumer.close()

if __name__ == '__main__':
    main()
