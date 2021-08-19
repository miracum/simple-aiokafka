import asyncio
import functools
from typing import Callable, Tuple, AsyncIterable
import aiokafka
import logging
from aiokafka.structs import ConsumerRecord
from aiokafka_handler.kafka_settings import KafkaSettings

log = logging.getLogger()


class AIOKafkaHandler:
    def __init__(self):
        self.conf = KafkaSettings()
        self.consumer_task: asyncio.Task = None
        self.consumer: aiokafka.AIOKafkaConsumer = None
        self.producer: aiokafka.AIOKafkaProducer = None

        # Set logging level from config variable 'kafka_log_level'
        logging.basicConfig(level=logging.getLevelName(self.conf.log_level.upper()))

    async def init_producer(self, output_topic: str = None):
        """Initiate AIOKafkaProducer instance with pydantic settings"""
        if output_topic is not None:
            self.conf.output_topic = output_topic
        self.producer = aiokafka.AIOKafkaProducer(
            loop=asyncio.get_event_loop(),
            max_request_size=self.conf.max_message_size_bytes,
            **self.conf.get_connection_context(),
            **self.conf.producer.dict(),
        )
        await self.producer.start()
        log.info(f"Initiated Kafka Producer: {self.conf.output_topic}")

    async def init_consumer(self, input_topic: str = None):
        """Initiate AIOKafkaConsumer instance with pydantic settings"""
        if input_topic is not None:
            self.conf.input_topic = input_topic
        self.consumer = aiokafka.AIOKafkaConsumer(
            self.conf.input_topic,
            loop=asyncio.get_event_loop(),
            **self.conf.get_connection_context(),
            **self.conf.consumer.dict(),
        )
        await self.consumer.start()
        log.info(f"Initiated Kafka Consumer: {self.conf.input_topic}")

    async def send(self, data: Tuple[str, str]):
        """Encode data to utf-8 and send it to the producer"""
        key = data[0].encode("utf-8") if data[1] else None
        value = data[1].encode("utf-8")
        await self.producer.send_and_wait(self.conf.output_topic, value, key)

    async def produce(self, iterable: AsyncIterable):
        async for data in iterable:
            await self.send(data)

    async def processor_task(self, func: Callable):
        self.consumer_task = asyncio.create_task(self.process(func))
        return self.consumer_task

    async def start_processor(self, func: Callable) -> asyncio.Task:
        # await asyncio.create_task(self.init_consumer())
        await self.init_producer()
        task = await self.process(func)
        return task

    async def stop_consuming(self):
        self.consumer_task.cancel()
        await self.consumer.stop()
        return await self.producer.stop()

    async def process(self, func: Callable):
        failed_topic = f"error.{self.conf.input_topic}.{self.conf.consumer.group_id}"
        log.info(f"{self.conf.input_topic} -> {self.conf.output_topic}")
        # consume messages
        msg: ConsumerRecord
        async for msg in self.consumer:
            log.debug(f"Consuming message: {msg}")
            log.info(f"Received message: {msg.key}")
            try:
                result: Tuple[str, str] = func(msg)
                await self.send(result)
                log.info(f"Sent message: {msg.key}")
            except Exception as err:
                log.exception(err)
                try:
                    await self.producer.send_and_wait(failed_topic, msg.value)
                except Exception as error_topic_exc:
                    log.exception(error_topic_exc)

    def __exit__(self):
        self.stop_consuming()


def kafka_consumer(input_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            kh = AIOKafkaHandler()
            await kh.init_consumer(input_topic)
            async for msg in kh.consumer:
                await func(msg)
            return await kh.consumer.stop()

        return wrapped

    return wrapper


def kafka_processor(input_topic: str = None, output_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args):
            kh = AIOKafkaHandler()
            await kh.init_consumer(input_topic)
            await kh.init_producer(output_topic)
            async for msg in kh.consumer:
                data = await func(msg)
                await kh.send(data=data)
            return await kh.consumer.stop()

        return wrapped

    return wrapper


def kafka_producer(output_topic: str = None) -> Callable:
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped():
            kh = AIOKafkaHandler()
            await kh.init_producer(output_topic)
            await kh.produce(func())

        return wrapped

    return wrapper
