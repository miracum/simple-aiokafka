import asyncio
from typing import Tuple, AsyncIterator
from simple_aiokafka import (
    SimpleConsumer,
    SimpleProducer,
    SimpleProcessor,
    ConsumerRecord,
)


def process_message(msg: ConsumerRecord):
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"


async def generate_message() -> AsyncIterator[Tuple[str, str]]:
    n = 0
    while True:
        yield str(n), f"Message {n}"
        n += 1
        await asyncio.sleep(1)


async def main():
    # SimpleProducer
    # generate_message -> dummy_topic
    producer = SimpleProducer()
    await producer.init("producer_topic")
    asyncio.create_task(producer.produce(generate_message()))

    # SimpleProcessor
    # dummy_topic -> SimpleProcessor -> process_message -> dummy_output_topic
    processor = SimpleProcessor()
    await processor.init(input_topic="producer_topic", output_topic="processor_topic")
    asyncio.create_task(processor.process(process_message))

    # SimpleConsumer
    # dummy_output_topic -> print
    consumer = SimpleConsumer()
    await consumer.init("processor_topic")
    async for msg in consumer.consumer:
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())
