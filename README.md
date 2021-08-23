# AIOKafkaHandler
AIOKafkaHandler is a simple wrapper for the [AIOKafka](https://github.com/aio-libs/aiokafka) library using [pydantic](https://github.com/samuelcolvin/pydantic) for easy configuration parsing.
It provides a convenient interface for Kafka Consumers, Producers and Processors.


## Classic API

Create an instance of `AIOKafkaHandler`, initiate the consumer/producer/processor and start processing!
For examples see [examples/classic_api.py](examples/classic_api.py).

### Consumer

~~~python
from simple_aiokafka import SimpleConsumer

sc = SimpleConsumer()
await sc.init()
async for msg in sc.consumer:
    print(msg)
~~~

### Producer
Simply call `AIOKafkaHandler.send((key, value))` in your loop:

~~~python
from simple_aiokafka import SimpleProducer

sp = SimpleProducer()
await sp.init()
for i in range(10):
    await sp.send(data=(str(i), "Value"))
~~~

or pass an __AsyncIterator__ object to `AioKafkaHandler.produce`:

~~~python
import asyncio
from simple_aiokafka import SimpleProducer

async def generate_message():
    n = 0
    while True:
        yield str(n), f"Message {n}"
        n += 1
        await asyncio.sleep(1)


sp = SimpleProducer()
await sp.init("dummy_topic")
await sp.produce(generate_message())
~~~

### Processor
A processor receives only a function that is executed on each incoming message on the consumer topic.
The result is sent to the producer topic.

~~~python
from simple_aiokafka import SimpleProcessor, ConsumerRecord

def process_message(msg: ConsumerRecord):
    return msg.key, f"{msg.value}: Hello Kafka :)"

processor = SimpleProcessor()
await processor.init(input_topic="dummy_topic", output_topic="dummy_output_topic")
await processor.process(process_message)
~~~


## Decorator Style API
To write even less boilerplate code, one can use the decorator API, similar to Spring Boot.

The Producer must be a Generator function that yields a tuple of strings.
These are passed to the AIOKafkaHandler.send method as key and value.

Import the relevant decorator:
~~~python
from typing import Tuple, AsyncIterator
from simple_aiokafka import (
    kafka_consumer, kafka_producer, kafka_processor, ConsumerRecord
)
~~~

#### Consumer
~~~python
@kafka_consumer(input_topic="processor_topic")
async def consume(msg: ConsumerRecord = None):
    print("Consume Message:", msg)
~~~

#### Producer
~~~python
@kafka_producer(output_topic="producer_topic")
async def produce() -> AsyncIterator[Tuple[str, str], None]:
    for i in range(100):
        yield str(i), f"Message {i}"
        await asyncio.sleep(1)
~~~

#### Processor
~~~python
@kafka_processor(input_topic="producer_topic", output_topic="processor_topic")
async def process(msg: ConsumerRecord = None) -> Tuple[str, str]:
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"
~~~
For a full example see [examples/decorator_api.py](examples/decorator_api.py).

## Configuration
One can set configuration variables via `export` or in the `.env` file.
These will be read by pydantic and stored in the `conf` object of your Consumer/Producer/Processor.
~~~bash
# Kafka settings
kafka_bootstrap_servers=localhost:9092
kafka_input_topic=test.input
kafka_output_topic=test.output
kafka_consumer_group_id=aiokafka_handler
~~~

Otherwise, the settings can be modified by setting the relevant value in the conf object:

~~~python
consumer = SimpleConsumer()
consumer.conf.consumer.group_id = "my_group_id"

producer = SimpleProducer()
producer.conf.producer.key_serializer = lambda x: str(x).encode()

processor = SimpleProcessor()
processor.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
processor.conf.producer.key_serializer = lambda x: str(x * 10).encode()
~~~

Settings for the underlying AIOKafka Client object can also be passed to the `init()` method of a
SimpleConsumer/SimpleProducer/SimpleProcessor:
~~~python
consumer = SimpleConsumer()
consumer.init(max_poll_records=10)
~~~

For all options see [aiokafka_handler/kafka_settings.py](simple_aiokafka/kafka_settings.py).


## Serialization
By default the Consumer de-serializes keys and values with `bytes.decode` and the Producer serializes with `str.encode`.

These defaults can be changed by setting them on the conf object
~~~python
producer.conf.producer.key_serializer = lambda x: str(x).encode()
# or
consumer.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
~~~

or by passing the respective key + value to the `init()` method.

## Development
### Install Requirements
```sh
python -m pip install -r requirements.dev.txt
```


### Contributing
```sh
python -m pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```


### Testing with kafkacat
Send a message to the input topic. See `kafkacat -X list` for more options.

Send a simple message to Kafka:
~~~bash
echo "Hello Kafka :)" | kafkacat -b localhost:9092 -t input_test -P
~~~

Send a message using SASL/SSL settings:
~~~bash
echo "Hello Kafka :)" | kafkacat -b localhost:9092 \
  -X security.protocol=sasl_plaintext \
  -X sasl.mechanisms=plain \
  -X sasl.username=user \
  -X sasl.password=password \
  -t input_test -P
~~~
