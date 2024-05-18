# This is a simple example of the SerializingProducer using Avro.
#
# import argparse
import configparser
from uuid import uuid4
import pandas as pd
import boto3
import io

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


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


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('Netflix event {} successfully produced to topic :{} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def get_csv_from_aws_s3(conf):
    """
    Downloade the csv data fils from AWS S3 bucket and return dataframe of than csv data.

    Args:
        args : all required configuration in dict format.

    retuens:
        Pandas Dataframe : returns csv data in from dataframe

    Note:


    """

    client = boto3.client(
        's3',
        aws_access_key_id=conf['aws_access_key'],
        aws_secret_access_key=conf['aws_secret_access_key'],
        region_name=conf['aws_region_name']
    )

    csv_file_object = client.get_object(Bucket= conf['aws_bucket_name'], Key=conf['aws_file_name'])

    csv_bytes = csv_file_object['Body'].read()

    # Create a DataFrame from the bytes
    return pd.read_csv(io.BytesIO(csv_bytes))


def main(args):
    topic = args['topic_name']

    schema_file_path = args['avro_schema_file_path']
    with open(schema_file_path, 'r') as schema_file:
        schema_str = schema_file.read()

    # print(schema_str)

    sr_conf = {'url': args['schema_registry']}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    string_serializer = StringSerializer('utf-8')
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     record_to_dict)

    producer_conf = {'bootstrap.servers': args['bootstrap_servers'],
                     'key.serializer': string_serializer,
                     'value.serializer': avro_serializer}

    producer = SerializingProducer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # Serve on_delivery callbacks from previous calls to produce()

    df = get_csv_from_aws_s3(args)

    for _, row in df.iterrows():
        producer.poll(20.0)
        try:

            # Produce records to the Kafka topic

            record = {
                "title": row['title'],
                "cast": row['cast'] if pd.notna(row['cast']) else None,
                "country": row['country'] if pd.notna(row['country']) else None,
                "date_added": row['date_added'] if pd.notna(row['date_added']) else None,
                "release_year": int(row['release_year']),
                "rating": row['rating'] if pd.notna(row['rating']) else None,
                "duration": row['duration'] if pd.notna(row['duration']) else None,
                "listed_in": row['listed_in'] if pd.notna(row['listed_in']) else None,
                "description": row['description'] if pd.notna(row['description']) else None
            }

            record_obj = Record(title = record['title'],
                                cast = record['cast'],
                                country = record['country'],
                                date_added = record['date_added'],
                                release_year = record['release_year'],
                                rating = record['rating'],
                                duration = record['duration'],
                                listed_in = record['listed_in'],
                                description = record['description'])

            producer.produce(topic=topic, key=str(uuid4()), value=record_obj,
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()

if __name__ == '__main__':

    parser = configparser.ConfigParser()
    parser.read("M:\\Training\\Kafka\\Kafka-Producer-Consumer\\credentials.conf")

    # All the required configuration information is to be found inside config dictionary

    config = {'bootstrap_servers': "localhost:9092",
              'schema_registry': "http://localhost:8081",
              'topic_name': "netflix_events",
              'aws_access_key': parser.get("default", "access_key"),
              'aws_secret_access_key': parser.get("default", "secret_access_key"),
              'aws_region_name': "us-east-1",
              'aws_bucket_name': "netflix-data-dump",
              'aws_file_name': "netflix_shows.csv",
              'avro_schema_file_path': "M:\\Training\\Kafka\\Kafka-Producer-Consumer\\pageviews.avsc"
              }

    main(config)