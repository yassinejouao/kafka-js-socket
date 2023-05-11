import time
import json
import random
from datetime import datetime
from data_generator import generate_message
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def serializer(message):
    return json.dumps(message).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9092']
)


topic = "user1"
existing_topics = admin_client.list_topics()
if topic not in existing_topics:
    new_topic = NewTopic(
        name=topic,
        num_partitions=1,
        replication_factor=1,
    )
    admin_client.create_topics([new_topic])

if __name__ == '__main__':

    i = 0
    while True:

        dummy_message = str(datetime.now())

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1



        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1

        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1
        print(f'message {i} @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)
        i+=1