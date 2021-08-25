import asyncio
import functools
import logging
from typing import Any, AsyncIterable, Callable, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from simple_aiokafka.kafka_settings import KafkaSettings

log = logging.getLogger()


class SimpleConsumer:
    def __init__(self, conf: KafkaSettings = None):
        self.conf = conf or KafkaSettings()
        self.consumer: AIOKafkaConsumer = None
        # Set logging level from config variable 'kafka_log_level'
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))
        log.debug(self.conf)

    async def init(self, input_topic: str = None, **kwargs):
        """Initialize AIOKafkaConsumer instance with pydantic settings"""

        # Permanently overwrite input_topic in KafkaSettings if given in args
        if input_topic:
            self.conf.input_topic = input_topic

        # Create context dictionary for the AIOKafkaConsumer args
        # The defaults from KafkaSettings are overwritten by **kwargs
        context = {
            "loop": asyncio.get_event_loop(),
            **self.conf.get_connection_context(),
            **self.conf.consumer.dict(),
            **kwargs,
        }

        # Instantiate AIOKafkaConsumer with context
        self.consumer = AIOKafkaConsumer(self.conf.input_topic, **context)
        await self.consumer.start()
        log.info(f"Initialized SimpleConsumer: {self.conf.output_topic}")

    async def stop(self):
        """Proxy method to AIOKafkaConsumer.stop()"""
        return await self.consumer.stop()


class SimpleProducer:
    def __init__(self, conf: KafkaSettings = None):
        self.conf = conf or KafkaSettings()
        self.producer: AIOKafkaProducer = None
        self.producer_task: asyncio.Task = None

    async def init(self, output_topic: str = None, **kwargs):
        """Initiate AIOKafkaProducer instance with pydantic conf"""
        if output_topic:
            self.conf.output_topic = output_topic

        context = {
            "loop": asyncio.get_event_loop(),
            **self.conf.get_connection_context(),
            **self.conf.producer.dict(),
            **kwargs,
        }

        self.producer = AIOKafkaProducer(**context)
        await self.producer.start()
        log.info(f"Initiated Kafka Producer: {self.conf.output_topic}")

    async def stop(self):
        self.producer_task.cancel()
        return await self.producer.stop()

    async def send(self, data: Tuple[Any, Any]):
        """Proxy to AIOKafkaProducer.send_and_wait, but catches errors"""
        try:
            await self.producer.send_and_wait(self.conf.output_topic, data[1], data[0])
        except Exception as err:
            log.exception(err)

    async def produce(self, iterable: AsyncIterable):
        async def producer_task():
            async for data in iterable:
                await self.send(data)

        self.producer_task = asyncio.create_task(producer_task())


class SimpleProcessor:
    def __init__(self):
        self.conf = KafkaSettings()
        self.consumer = SimpleConsumer(self.conf)
        self.producer = SimpleProducer(self.conf)
        self.producer_task: asyncio.Task = None
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))

    async def init(
        self,
        input_topic: str = None,
        output_topic: str = None,
        consumer_args: dict = None,
        producer_args: dict = None,
    ):
        await self.consumer.init(input_topic, **consumer_args or {})
        await self.producer.init(output_topic, **producer_args or {})
        log.info("Initialized SimpleProcessor")

    async def stop(self):
        await self.consumer.stop()
        await self.consumer.stop()

    async def process(self, func: Callable):
        log.info(f"{self.conf.input_topic} -> {self.conf.output_topic}")

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
                self.conf.dlq_topic, msg.value, msg.key
            )
        except Exception as error_topic_exc:
            log.exception(error_topic_exc)


def kafka_consumer(input_topic: str = None, **kwargs) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            consumer = SimpleConsumer()
            await consumer.init(input_topic, **kwargs)
            async for msg in consumer.consumer:
                await func(msg)
            return await consumer.stop()

        return wrapped

    return wrapper


def kafka_processor(
    input_topic: str = None,
    output_topic: str = None,
    producer_args: dict = None,
    consumer_args: dict = None,
) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args):
            processor = SimpleProcessor()
            await processor.consumer.init(input_topic, **consumer_args or {})
            await processor.producer.init(output_topic, **producer_args or {})
            async for msg in processor.consumer.consumer:
                data = await func(msg)
                await processor.producer.send(data=data)
            return await processor.stop()

        return wrapped

    return wrapper


def kafka_producer(output_topic: str = None, **kwargs) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            producer = SimpleProducer()
            await producer.init(output_topic, **kwargs)
            await producer.produce(func())

        return wrapped

    return wrapper
