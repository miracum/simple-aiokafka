import asyncio
from typing import Tuple, AsyncIterator
from simple_aiokafka import (
    SimpleConsumer,
    SimpleProducer,
    SimpleProcessor,
    ConsumerRecord,
)


def process_message(msg: ConsumerRecord):
    return msg.key, f"{msg.value}: Hello Kafka :)"


async def generate_message() -> AsyncIterator[Tuple[str, str]]:
    n = 0
    while True:
        yield n, f"Message {n}"
        n += 1
        await asyncio.sleep(1)


async def main():
    # generate_message -> dummy_topic
    producer = SimpleProducer()
    # Modify AIOKafkaProducer key_serializer
    producer.conf.producer.key_serializer = lambda x: str(x).encode()
    await producer.init("producer_topic")
    asyncio.create_task(producer.produce(generate_message()))

    # dummy_topic -> SimpleProcessor -> process_message -> dummy_output_topic
    processor = SimpleProcessor()
    processor.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
    processor.conf.producer.key_serializer = lambda x: str(x * 10).encode()
    await processor.init(
        input_topic="producer_topic",
        output_topic="processor_topic",
    )
    asyncio.create_task(processor.process(process_message))

    # dummy_output_topic -> print
    consumer = SimpleConsumer()
    consumer.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
    consumer.conf.consumer.group_id = "MyGroup"
    await consumer.init("processor_topic")
    async for msg in consumer.consumer:
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())
