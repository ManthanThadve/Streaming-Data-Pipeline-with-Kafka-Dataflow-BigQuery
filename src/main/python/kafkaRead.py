from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry.avro import AvroDeserializer

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer



class Record(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential

    """
    def __init__(self, title,cast,country,date_added,release_year,rating,duration,listed_in,description):
        self.title = title
        self.cast = cast
        self.country = country
        self.date_added = date_added
        self.release_year = release_year
        self.rating = rating
        self.duration = duration
        self.listed_in = listed_in
        self.description = description


def record_to_dict(record, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.

    """
    if record is None:
        return None

    # User._address must not be serialized; omit from dict
    return dict(title = record.title,
                cast = record.cast,
                country = record.country,
                date_added = record.date_added,
                release_year = record.release_year,
                rating = record.rating,
                duration = record.duration,
                listed_in = record.listed_in,
                description = record.description)


# Replace with VM's hostname or IP address, adjust port if needed
bootstrap_servers = 'localhost:9092'
schema_registry = 'http://localhost:8081'

# Replace with the topic name you want to consume from
topic_name = 'netflix_events'

# Consumer group ID (can be any string)
group_id = 'Nexflix_consumer'

schema_file_path = "M:\\Training\\Kafka\\Kafka-Producer-Consumer\\pageviews.avsc"
with open(schema_file_path, 'r') as schema_file:
    schema_str = schema_file.read()

sr_conf = {'url': schema_registry}
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_deserializer = AvroDeserializer(schema_registry_client,
                                     schema_str)
string_deserializer = StringDeserializer('utf-8')

consumer_conf = {'bootstrap.servers': bootstrap_servers,
                 'key.deserializer': string_deserializer,
                 'value.deserializer': avro_deserializer,
                 'group.id': group_id,
                 'auto.offset.reset': "earliest"}

consumer = DeserializingConsumer(consumer_conf)

consumer.subscribe([topic_name])

try:
    while True:

        try:
            msg = consumer.poll(timeout=20.0)  # Poll for new messages with a 1-second timeout
            if msg is None:
                print("Waiting.....")
                continue

            elif msg.error():
                print("Error: %s" % msg.error())

            else:
                # print("Received message: %s" % msg.value().decode('utf-8'))  # Assuming UTF-8 encoding
                print(f"Recived message {msg.key()} : {msg.value()}")

        except SerializerError as e:
            print(f"Message deserialization failed: {e}")
            continue

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
