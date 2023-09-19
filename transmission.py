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

class TransmissionConfig(BaseModel):
    host: str
    user: str
    password: str
    exchanges: List[MessageExchange] = Field(default_factory=list)
    queues: List[MessageQueue] = Field(default_factory=list)

async def main(config: TransmissionConfig):
    try:
        try:
            connection = await aio_pika.connect(
                f"amqp://{config.user}:{config.password}@{config.host}/"
            )
        except ProbableAuthenticationError:
            exit_on_error("Login to RabbitMQ failed: please check your username and password")
        except AMQPConnectionError:
            exit_on_error("Connection failed: unable to connect to the RabbitMQ server")
        except Exception as e:
            exit_on_error(f"An unexpected error occurred while connecting to RabbitMQ server: {e}")

        for exchange_data in config.exchanges:
            await set_up_exchange(connection,exchange_data)

        for queue_data in config.queues:
            await set_up_queue(connection,queue_data)

            for bind_data in queue_data.binds:
                await set_up_bind(connection, queue_data.name, bind_data)
                
        print("Finished executing.", file=sys.stderr)
    except asyncio.CancelledError:
        print("Execution was cancelled prematurely.", file=sys.stderr)

async def set_up_exchange(connection: Connection, exchange_data: MessageExchange):
    try:
        channel = await connection.channel()
        try:
            exchange = await channel.declare_exchange(exchange_data.name, passive=True)
            print(f"Exchange with name '{exchange.name}' already exists.", file=sys.stderr)
        except ChannelNotFoundEntity:
            channel = await connection.channel()
            exchange = await channel.declare_exchange(exchange_data.name,type=exchange_data.type.value,durable=exchange_data.durable,passive=False)
            print(f"Exchange with name '{exchange.name}' declared.", file=sys.stderr)
    except ChannelAccessRefused:
        print(f"Channel has refused access on exchange '{exchange_data.name}'. Check you login configuration and user permission.", file=sys.stderr)

async def set_up_queue(connection: Connection, queue_data: MessageQueue):
    try:
        channel = await connection.channel()
        try:
            queue = await channel.declare_queue(queue_data.name, passive=True)
            print(f"Queue with name '{queue.name}' already exists.", file=sys.stderr)
        except ChannelNotFoundEntity:
            channel = await connection.channel()
            queue = await channel.declare_queue(queue_data.name, durable=queue_data.durable, passive=False)
            print(f"Queue with name '{queue.name}' declared.", file=sys.stderr)
    except ChannelAccessRefused:
        print(f"Channel has refused access on queue '{queue_data.name}'. Check you login configuration and user permission.", file=sys.stderr)

async def set_up_bind(connection: Connection, queue_name: str, bind_data: MessageBind):
    try:
        channel = await connection.channel()
        try:
            exchange = await channel.get_exchange(bind_data.exchange_name)
        except ChannelNotFoundEntity:
            print(f"Exchange with name '{bind_data.exchange_name}' does not exist. Check your configuration file.", file=sys.stderr)
            return
        try:
            queue = await channel.get_queue(queue_name)
        except ChannelNotFoundEntity:
            print(f"Queue with name '{queue_name}' does not exist. Check your configuration file.", file=sys.stderr)
            return
        await queue.bind(exchange.name,bind_data.routing_key)
        print(f"Queue with name '{queue_name}' bound to exchange {bind_data.exchange_name} with routing key '{bind_data.routing_key}'.", file=sys.stderr)
    except ChannelAccessRefused:
        print(f"Channel has refused access binding queue '{queue_name}' with exchange '{bind_data.exchange_name}'. Check you login configuration and user permission.", file=sys.stderr)
        channel = await connection.channel() # Not reconnecting here will cause the program to crash on the last bind (even though it properly sets it). No clue why.

if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file_path = sys.argv[1]
    else:
        exit_on_error("Expected configuration file path as argument.")
    try:
        with open(config_file_path, "rb") as file:
            config_dict = tomli.load(file)
            config = TransmissionConfig(**config_dict)
    except ValidationError as e:
        exit_on_error(f"Failed to validate configuration: {e}")
    except tomli.TOMLDecodeError as e:
        exit_on_error(f"Failed to parse TOML file: {e}")
    except FileNotFoundError:
        exit_on_error(f"File not found: {config_file_path}")

    asyncio.run(main(config))