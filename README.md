# AIOKafkaHandler
AIOKafkaHandler is a simple wrapper for the [AIOKafka](https://github.com/aio-libs/aiokafka) library using [pydantic](https://github.com/samuelcolvin/pydantic) for easy configuration parsing.
It provides a convenient interface for Kafka Consumers, Producers and Processors.


## Classic API

Create an instance of `AIOKafkaHandler`, initiate the consumer/producer/processor and start processing!
For examples see [examples/classic_api.py](examples/classic_api.py).

### Consumer
~~~python
from aiokafka_handler.kafka_handler import AIOKafkaHandler

kh = AIOKafkaHandler()
await kh.init_consumer()
async for msg in kh.consumer:
    print(msg)
~~~

### Producer
Simply call `AIOKafkaHandler.send((key, value))` in your loop:
~~~python
from aiokafka_handler.kafka_handler import AIOKafkaHandler

kh = AIOKafkaHandler()
await kh.init_producer()
for i in range(10):
    await kh.send(data=(str(i), "Value"))
~~~

or pass an __AsyncIterator__ object to `AioKafkaHandler.produce`:
~~~python
import asyncio
from typing import Tuple, AsyncIterator
from aiokafka_handler.kafka_handler import AIOKafkaHandler, ConsumerRecord

async def generate_message() -> AsyncIterator[Tuple[str, str]]:
    n = 0
    while True:
        yield str(n), f"Message {n}"
        n += 1
        await asyncio.sleep(1)

kh = AIOKafkaHandler()
await kh.init_producer("dummy_topic")
await kh.produce(generate_message())
~~~

### Processor
A processor receives only a function that is executed on each incoming message on the consumer topic.
The result is sent to the producer topic.
~~~python
from aiokafka_handler.kafka_handler import AIOKafkaHandler, ConsumerRecord

def process_message(msg: ConsumerRecord):
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"

kh = AIOKafkaHandler()
await kh.init_consumer("dummy_topic")
await kh.init_producer("dummy_output_topic")
await kh.process(process_message)
~~~


## Decorator Style API
To write even less boilerplate code, one can use the decorator API, similar to Spring Boot.

The Producer must be a Generator function that yields a tuple of strings.
These are passed to the AIOKafkaHandler.send method as key and value.

~~~python
from typing import Tuple, AsyncIterator
from aiokafka_handler.kafka_handler import (
    kafka_consumer, kafka_producer, kafka_processor, ConsumerRecord
)

# Producer
@kafka_producer(output_topic="producer_topic")
async def produce() -> AsyncIterator[Tuple[str, str], None]:
    for i in range(100):
        yield str(i), f"Message {i}"
        await asyncio.sleep(1)

# Processor
@kafka_processor(input_topic="producer_topic", output_topic="processor_topic")
async def process(msg: ConsumerRecord = None) -> Tuple[str, str]:
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"

# Consumer
@kafka_consumer(input_topic="processor_topic")
async def consume(msg: ConsumerRecord = None):
    print("Consume Message:", msg)
~~~

For a full example see [examples/decorator_api.py](examples/decorator_api.py).

### Configure
Set your variables via `export` or in your `.env` file.
For all options see [aiokafka_handler/kafka_settings.py](aiokafka_handler/kafka_settings.py).

~~~bash
# Kafka settings
kafka_bootstrap_servers=localhost:9092
kafka_input_topic=test.input
kafka_output_topic=test.output
kafka_consumer_group_id=aiokafka_handler
~~~


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
