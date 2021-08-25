import os
import ssl

from aiokafka.helpers import create_ssl_context

import simple_aiokafka.simple_aiokafka
from simple_aiokafka.kafka_settings import KafkaSettings


def test_simple_consumer():
    c = simple_aiokafka.SimpleConsumer()
    c.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
    assert c.conf.consumer.key_deserializer(b"12") == 12


def test_simple_producer():
    p = simple_aiokafka.SimpleProducer()
    p.conf.producer.key_serializer = lambda x: str(str(x) + "123").encode()
    assert p.conf.producer.key_serializer("TestString") == b"TestString123"


def test_simple_processor():
    p = simple_aiokafka.SimpleProcessor()
    p.conf.consumer.key_deserializer = lambda x: int(bytes.decode(x))
    p.conf.producer.key_serializer = lambda x: str(str(x) + "123").encode()
    assert p.conf.producer.key_serializer("TestString") == b"TestString123"
    assert p.conf.consumer.key_deserializer(b"12") == 12


test_connection_context = {
    "ssl_context": create_ssl_context(cafile=None, certfile=None, keyfile=None),
    "bootstrap_servers": "kafka1:9092",
    "security_protocol": "SASL_SSL",
    "sasl_plain_username": "test",
    "sasl_plain_password": "testpw",
    "sasl_mechanism": "SASL_PLAIN",
}


def test_settings_env_file():
    os.environ["PYDANTIC_ENV_FILE"] = ".env.development"
    conf = KafkaSettings()
    assert conf.log_level == "debug"


def test_settings():
    conf = KafkaSettings()
    assert conf.get_ssl_context() is None

    conf.bootstrap_servers = "kafka1:9092"
    conf.sasl_mechanism = "SASL_PLAIN"
    conf.security_protocol = "SASL_SSL"
    conf.sasl_plain_username = "test"
    conf.sasl_plain_password = "testpw"
    conf.ssl_cafile = None
    conf.ssl_certfile = None
    conf.ssl_keyfile = None

    context = conf.get_connection_context()
    test_connection_context["ssl_context"] = context["ssl_context"]
    assert isinstance(conf.get_ssl_context(), ssl.SSLContext)
    assert context == test_connection_context


def dummy_consumer(msg=None):
    return msg


def test_kafka_consumer():
    wrapper = simple_aiokafka.kafka_consumer("test_topic", group_id="myGroup")
    wrapped = wrapper(dummy_consumer)
    assert wrapped.__name__ == "dummy_consumer"
    assert wrapped.__wrapped__.__name__ == "dummy_consumer"


def test_kafka_producer():
    wrapper = simple_aiokafka.kafka_producer("test_topic")
    wrapped = wrapper(dummy_consumer)
    assert wrapped.__name__ == "dummy_consumer"
    assert wrapped.__wrapped__.__name__ == "dummy_consumer"


def test_kafka_processor():
    wrapper = simple_aiokafka.kafka_processor("input_topic", "output_topic")
    wrapped = wrapper(dummy_consumer)
    assert wrapped.__name__ == "dummy_consumer"
    assert wrapped.__wrapped__.__name__ == "dummy_consumer"
