# AIOKafkaHandler
AIOKafkaHandler is a simple wrapper for the great [AIOKafka](https://github.com/aio-libs/aiokafka) library using [pydantic](https://github.com/samuelcolvin/pydantic) for configuration.
It provides a convenient interface for Kafka Consumer, Producer and Processors.


## Classic API

Create an instance of `AIOKafkaHandler`, initiate the consumer/producer/processor and start processing:

### Consumer
~~~python
from aiokafka_handler.kafka_handler import AIOKafkaHandler

kh = AIOKafkaHandler()
await kh.init_consumer()
async for msg in kh.consumer:
    print(msg)
~~~

### Producer

~~~python
from aiokafka_handler.kafka_handler import AIOKafkaHandler

kh = AIOKafkaHandler()
await kh.init_producer()
for i in range(10):
    kh.send(data=(str(i), "Value"))
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
~~~python
from typing import Tuple
from aiokafka_handler.kafka_handler import (
    kafka_consumer, kafka_producer, kafka_processor, ConsumerRecord
)

@kafka_consumer("processor_topic")
async def consume(msg: ConsumerRecord = None):
    print("Consume Message:", msg)

@kafka_processor(input_topic="producer_topic", output_topic="processor_topic")
async def process(msg: ConsumerRecord = None) -> Tuple[str, str]:
    return str(msg.key.decode()), f"{msg.value.decode()}: Hello Kafka :)"


@kafka_producer("producer_topic")
async def produce() -> Tuple[str, str]:
    for i in range(100):
        yield str(i), f"Message {i}"
        await asyncio.sleep(1)
~~~

For a full example see [example-decorators.py](example_decorators.py).

### Configure
Set your variables via `export` or in your `.env` file.
For all options see [aiokafka_handler/kafka_settings.py](aiokafka_handler/kafka_settings.py).

~~~bash
# Kafka settings
kafka_bootstrap_servers=localhost:9092
kafka_input_topic=input_test
kafka_output_topic=input_test
kafka_consumer_group_id=aiokafka_handler
~~~

### Run
~~~bash
python kafka2api/main.py
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


### Test Kafka

Send a message to the input topic. See `kafkacat -X list` for more options.

~~~bash
tail -n1 mock-data.ndjson | kafkacat -b localhost:9092 \
  -X security.protocol=sasl_plaintext \
  -X sasl.mechanisms=plain \
  -X sasl.username=user \
  -X sasl.password=password \
  -t input_test -P
~~~

~~~bash
tail -n1 mock-data.ndjson | kafkacat -b localhost:9092 \
  -t input_test -P
~~~
