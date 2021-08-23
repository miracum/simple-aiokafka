import asyncio
import functools
from typing import Callable, Tuple, AsyncIterable
import aiokafka
import logging
from aiokafka.structs import ConsumerRecord
from simple_aiokafka.kafka_settings import (
    SimpleConsumerSettings,
    SimpleProducerSettings,
    KafkaSettings,
)

log = logging.getLogger()


class SimpleConsumer:
    def __init__(self, **kwargs):
        self.conf = SimpleConsumerSettings()
        self.consumer: aiokafka.AIOKafkaConsumer = None
        # Set logging level from config variable 'kafka_log_level'
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))

    async def init(self, input_topic: str = None):
        """Initiate AIOKafkaConsumer instance with pydantic conf"""
        if input_topic is not None:
            self.conf.input_topic = input_topic
        self.consumer = aiokafka.AIOKafkaConsumer(
            self.conf.input_topic,
            loop=asyncio.get_event_loop(),
            **self.conf.get_connection_context(),
            **self.conf.consumer.dict(),
        )
        await self.consumer.start()
        log.info(f"Started KafkaConsumer: {self.conf.input_topic}")

    async def stop(self):
        return await self.consumer.stop()


class SimpleProducer:
    def __init__(self):
        self.conf = SimpleProducerSettings()
        self.producer: aiokafka.AIOKafkaProducer = None
        self.producer_task: asyncio.Task = None
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))

    async def init(self, output_topic: str = None):
        """Initiate AIOKafkaProducer instance with pydantic conf"""
        if output_topic is not None:
            self.conf.topic = output_topic
        print(self.conf.dict())
        self.producer = aiokafka.AIOKafkaProducer(
            loop=asyncio.get_event_loop(),
            **self.conf.get_connection_context(),
            **self.conf.dict(),
        )
        await self.producer.start()
        log.info(f"Initiated Kafka Producer: {self.conf.topic}")

    async def stop(self):
        self.producer_task.cancel()
        return await self.producer.stop()

    async def send(self, data: Tuple[str, str]):
        """Encode data to utf-8 and send it to the producer"""
        try:
            key = data[0].encode("utf-8") if data[0] else None
            value = data[1].encode("utf-8")
            try:
                await self.producer.send_and_wait(self.conf.topic, value, key)
            except Exception as err:
                log.exception(err)
        except Exception as err:
            log.error(f"Cannot format message to bytes: {data}")
            log.error(repr(err))

    async def produce(self, iterable: AsyncIterable):
        async def producer_task():
            async for data in iterable:
                await self.send(data)

        self.producer_task = asyncio.create_task(producer_task())


class SimpleProcessor:
    def __init__(self):
        self.conf = KafkaSettings()
        self.producer_task: asyncio.Task = None
        self.consumer = SimpleConsumer()
        self.producer = SimpleProducer()
        # Set logging level from config variable 'kafka_log_level'
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))

    async def init(self, input_topic: str = None, output_topic: str = None):
        await self.consumer.init(input_topic)
        await self.producer.init(output_topic)

    async def stop(self):
        await self.consumer.stop()
        await self.consumer.stop()

    async def process(self, func: Callable):
        log.info(f"{self.conf.input_topic} -> {self.conf.topic}")

        async for msg in self.consumer.consumer:
            log.debug(f"Consumed: {msg.key}: {msg.value[:80]}")
            try:
                result: Tuple[str, str] = func(msg)
                await self.producer.send(result)
                log.debug(f"Produced: {msg.key}: {msg.value[:80]}")
            except Exception as err:
                log.error(repr(err))
                if self.conf.send_errors_to_dlq:
                    await self.send_dlq(msg)

    async def send_dlq(self, msg: ConsumerRecord):
        try:
            await self.producer.producer.send_and_wait(
                self.settings.dlq_topic, msg.value, msg.key
            )
        except Exception as error_topic_exc:
            log.exception(error_topic_exc)


def kafka_consumer(input_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            consumer = SimpleConsumer()
            await consumer.init(input_topic)
            print("asdasdasd", consumer.consumer)
            async for msg in consumer.consumer:
                await func(msg)
            return await consumer.stop()

        return wrapped

    return wrapper


def kafka_processor(input_topic: str = None, output_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args):
            processor = SimpleProcessor()
            await processor.consumer.init(input_topic)
            await processor.producer.init(output_topic)
            async for msg in processor.consumer.consumer:
                data = await func(msg)
                await processor.producer.send(data=data)
            return await processor.stop()

        return wrapped

    return wrapper


def kafka_producer(output_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            producer = SimpleProducer()
            await producer.init(output_topic)
            await producer.produce(func())

        return wrapped

    return wrapper
