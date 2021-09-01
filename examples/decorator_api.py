import asyncio
from typing import AsyncGenerator, Tuple

from pydantic import BaseModel

from simple_aiokafka import (
    ConsumerRecord,
    kafka_consumer,
    kafka_processor,
    kafka_producer,
)


class Document(BaseModel):
    text: str
    id: int
    note: str = None


def document_serializer(document: Document):
    return document.json().encode()


@kafka_producer(value_serializer=document_serializer)
async def produce() -> AsyncGenerator[Tuple[str, Document], None]:
    for i in range(100):
        yield str(i), Document(text="Hello Kafka", id=i)
        await asyncio.sleep(1)


@kafka_consumer("aiokafka.result", value_deserializer=Document.parse_raw)
async def consume(msg: ConsumerRecord = None) -> None:
    print("Consume Message:", msg)


@kafka_processor(
    input_topic="aiokafka.output",
    output_topic="aiokafka.result",
    consumer_args={"value_deserializer": Document.parse_raw},
    producer_args={"value_serializer": document_serializer},
)
async def process(msg: ConsumerRecord = None) -> Tuple[str, str]:
    document = msg.value
    document.note = "Hello Kafka :)"
    return msg.key, document


async def main():
    asyncio.create_task(produce())
    asyncio.create_task(process())
    await consume()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
