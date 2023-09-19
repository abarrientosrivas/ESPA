from pydantic import BaseModel, ValidationError, Field
from typing import List
from enum import Enum
from aio_pika.exceptions import ProbableAuthenticationError, AMQPConnectionError, ChannelNotFoundEntity, ChannelClosed
from aiormq.exceptions import ChannelAccessRefused
from aio_pika import Connection
from espa import exit_on_error
import tomli, asyncio, sys, aio_pika

class ExchangeType(Enum):
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"

class MessageBind(BaseModel):
    exchange_name: str
    routing_key: str

class MessageExchange(BaseModel):
    name: str
    type: ExchangeType = ExchangeType.FANOUT
    durable: bool = False

class MessageQueue(BaseModel):
    name: str
    durable: bool = False
    binds: List[MessageBind] = Field(default_factory=list)

class DocumentMemoriesConfig(BaseModel):
    host: str
    user: str
    password: str
    assimilate_file_mq: str

if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file_path = sys.argv[1]
    else:
        exit_on_error("Expected configuration file path as argument.")
    try:
        with open(config_file_path, "rb") as file:
            config_dict = tomli.load(file)
            config = DocumentMemoriesConfig(**config_dict)
    except ValidationError as e:
        exit_on_error(f"Failed to validate configuration: {e}")
    except tomli.TOMLDecodeError as e:
        exit_on_error(f"Failed to parse TOML file: {e}")
    except FileNotFoundError:
        exit_on_error(f"File not found: {config_file_path}")

    asyncio.run(main(config))