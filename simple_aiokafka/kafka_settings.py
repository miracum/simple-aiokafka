import os
from enum import Enum
from typing import Callable

from aiokafka.helpers import create_ssl_context
from pydantic import BaseSettings, validator


class SecurityProtocolEnum(str, Enum):
    plaintext = "PLAINTEXT"
    ssl = "SASL_SSL"


class SaslMechanismEnum(str, Enum):
    plain = "PLAIN"
    gssapi = "GSSAPI"
    scram_sha_256 = "SCRAM-SHA-256"
    scram_sha_512 = "SCRAM-SHA-512"
    oauthbearer = "OAUTHBEARER"


class KafkaConsumerSettings(BaseSettings):
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 15000
    max_poll_records: int = 5
    max_poll_interval_ms: int = 600_000  # 10 minutes
    heartbeat_interval_ms: int = 3000
    auto_offset_reset: str = "earliest"
    group_id: str = "SimpleAIOKafka"
    key_deserializer: Callable = bytes.decode
    value_deserializer: Callable = bytes.decode

    class Config:
        env_prefix = "kafka_consumer_"


class KafkaProducerSettings(BaseSettings):
    compression_type: str = "gzip"
    max_request_size: int = 5242880
    key_serializer: Callable = str.encode
    value_serializer: Callable = str.encode

    class Config:
        env_prefix = "kafka_producer_"


class KafkaSettings(BaseSettings):
    consumer = KafkaConsumerSettings()
    producer = KafkaProducerSettings()
    log_level: str = "warning"
    input_topic: str = "aiokafka.input"
    output_topic: str = "aiokafka.output"
    bootstrap_servers: str = "localhost:9092"
    send_errors_to_dlq: bool = True
    dlq_topic: str = f"error.{input_topic}.{consumer.group_id}"
    # SSL Settings
    security_protocol: SecurityProtocolEnum = SecurityProtocolEnum.plaintext
    tls_dir: str = "/opt/"
    ssl_cafile: str = os.path.join(tls_dir, "ca.crt")
    ssl_certfile: str = os.path.join(tls_dir, "user.crt")
    ssl_keyfile: str = os.path.join(tls_dir, "user.key")
    # SASL Settings
    sasl_plain_username: str = None
    sasl_plain_password: str = None
    sasl_mechanism: SaslMechanismEnum = None

    class Config:
        env_prefix = "kafka_"
        env_file = os.getenv(
            "PYDANTIC_ENV_FILE",
            os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
        )

    def get_ssl_context(self):
        if self.security_protocol != "PLAINTEXT":
            return create_ssl_context(
                cafile=self.ssl_cafile,
                certfile=self.ssl_certfile,
                keyfile=self.ssl_keyfile,
            )
        return None

    def get_connection_context(self):
        return {
            "ssl_context": self.get_ssl_context(),
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_plain_username": self.sasl_plain_username,
            "sasl_plain_password": self.sasl_plain_password,
            "sasl_mechanism": self.sasl_mechanism,
        }

    # For using SASL without SSL certificates the *file args need to be None.
    # Otherwise AIOKafkaClient will try to parse them even if they
    # consist of an empty string.
    @validator("ssl_cafile", "ssl_certfile", "ssl_keyfile")
    def parse_to_none(cls, v):
        return None if v in ["", "None", 0, False] else v


c = KafkaSettings()
print(c)
