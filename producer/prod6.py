import time
import json
import random
from datetime import datetime
from data_generator import generate_message
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9092']
)

# Create the topic if it does not exist
topic = "user6"
existing_topics = admin_client.list_topics()
if topic not in existing_topics:
    new_topic = NewTopic(
        name=topic,
        num_partitions=1,
        replication_factor=1,
    )
    admin_client.create_topics([new_topic])

if __name__ == '__main__':
    # Infinite loop - runs until you kill the program
    while True:
        # Generate a message
        # dummy_message = generate_message()
        dummy_message = str(datetime.now())

        # Send it to our 'messages' topic
        print(f'message @ {datetime.now()} | Message = {str(dummy_message)}')
        producer.send(topic, dummy_message)

        # Sleep for a random number of seconds
        time_to_sleep = random.randint(1, 1)
        time.sleep(0.01)