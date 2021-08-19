import asyncio
from typing import Tuple

from aiokafka_handler.kafka_handler import AIOKafkaHandler, ConsumerRecord, \
    kafka_producer


def do_something_with_message(msg: ConsumerRecord):
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"


async def dummy_producer() -> Tuple[str, str]:
    for i in range(100):
        print("asd", i)
        yield str(i), f"Message {i}"
        await asyncio.sleep(1)


async def main():
    # Create AIOKafkaHandler Instance for the producer
    producer = AIOKafkaHandler()
    await producer.init_producer()
    # Run producer task in background
    asyncio.create_task(producer.produce(dummy_producer()))

    # Create AIOKafkaHandler instance for the processor
    processor = AIOKafkaHandler()
    await processor.init_consumer("dummy_topic")
    await processor.init_producer()
    await processor.process(do_something_with_message)

    consumer = AIOKafkaHandler().init_consumer()

if __name__ == "__main__":
    asyncio.run(main())
