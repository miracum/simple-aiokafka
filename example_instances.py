import asyncio
from typing import Tuple, AsyncIterator
from aiokafka_handler.kafka_handler import AIOKafkaHandler, ConsumerRecord


def process_message(msg: ConsumerRecord):
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"


async def generate_message() -> AsyncIterator[Tuple[str, str]]:
    n = 0
    while True:
        yield str(n), f"Message {n}"
        n += 1
        await asyncio.sleep(1)


async def main():
    # Producer
    # generate_message -> dummy_topic
    producer = AIOKafkaHandler()
    await producer.init_producer("dummy_topic")
    asyncio.create_task(producer.produce(generate_message()))

    # Processor
    # dummy_topic -> Processor -> process_message -> dummy_output_topic
    processor = AIOKafkaHandler()
    await processor.init_consumer("dummy_topic")
    await processor.init_producer("dummy_output_topic")
    asyncio.create_task(processor.process(process_message))

    # Consumer
    # dummy_output_topic -> print
    consumer = AIOKafkaHandler()
    await consumer.init_consumer("dummy_output_topic")
    async for msg in consumer.consumer:
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())
