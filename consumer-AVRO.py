import asyncio

from confluent_kafka import Consumer, avro
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    #     Create a CachedSchemaRegistryClient
    #
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    #
    #     Use the Avro Consumer
    #
    c = AvroConsumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "0"},
        schema_registry=schema_registry,
    )
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                #The print from console
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("email"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(consume(topic_name))
    await t1



if __name__ == "__main__":
    main()
