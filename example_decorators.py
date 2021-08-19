import asyncio
from typing import Tuple
from aiokafka_handler.kafka_handler import kafka_consumer, kafka_producer
from aiokafka_handler.kafka_handler import kafka_processor, ConsumerRecord


@kafka_consumer("processor_topic")
async def consume(msg: ConsumerRecord = None) -> None:
    print("Consume Message:", msg)

@kafka_processor(input_topic="producer_topic", output_topic="processor_topic")
async def process(msg: ConsumerRecord = None) -> Tuple[str, str]:
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"


@kafka_producer("producer_topic")
async def produce() -> Tuple[str, str]:
    for i in range(100):
        yield str(i), f"Message {i}"
        await asyncio.sleep(1)

async def main():
    asyncio.create_task(produce())
    asyncio.create_task(process())
    await consume()

if __name__ == "__main__":
    asyncio.run(main())
