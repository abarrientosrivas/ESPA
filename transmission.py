from pydantic import BaseModel
from typing import List
from aio_pika.exceptions import ProbableAuthenticationError, AMQPConnectionError, ChannelNotFoundEntity
import tomli, asyncio, sys, aio_pika

class MessageBind(BaseModel):
    exchange_name: str
    routing_key: str

class MessageExchange(BaseModel):
    name: str
    type: str
    durable: bool

class MessageQueue(BaseModel):
    name: str
    binds: List[MessageBind]

class TransmissionConfig(BaseModel):
    host: str
    user: str
    password: str
    exchanges: List[MessageExchange]
    queues: List[MessageQueue]

def exit_on_error(message:str):
    print(message, file=sys.stderr)
    sys.exit()

async def main(config: TransmissionConfig):
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

    channel = await connection.channel()

    for exchange_data in config.exchanges:
        try:
            await channel.declare_exchange(exchange_data.name, passive=True)
            print(f"Exchange with name {exchange_data.name} already exists.", file=sys.stderr)
        except ChannelNotFoundEntity as e:
            channel = await connection.channel()
            await channel.declare_exchange(exchange_data.name,type=exchange_data.type,durable=exchange_data.durable,passive=False)
            print(f"Exchange with name {exchange_data.name} declared.", file=sys.stderr)

    for queue_data in config.queues:
        try:
            queue = await channel.declare_queue(queue_data.name, passive=True)
            print(f"Queue with name {queue.name} already exists.", file=sys.stderr)
        except ChannelNotFoundEntity as e:
            channel = await connection.channel()
            queue = await channel.declare_queue(queue_data.name, passive=False)
            print(f"Queue with name {queue.name} declared.", file=sys.stderr)

        for bind in queue_data.binds:
            try:
                exchange = await channel.get_exchange(bind.exchange_name)
                await queue.bind(exchange,bind.routing_key)
                print(f"Queue with name {queue_data.name} bound to exchange {bind.exchange_name} with routing key {bind.routing_key}.", file=sys.stderr)
            except ChannelNotFoundEntity as e:
                channel = await connection.channel()
                print(f"Exchange with name {bind.exchange_name} does not exist. Check your configuration file.", file=sys.stderr)
            
    print("done")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        config_file_path = sys.argv[1]
    else:
        exit_on_error("Expected configuration file path as argument.")
    try:
        with open(config_file_path, "rb") as f:
            config_dict = tomli.load(f)
            config = TransmissionConfig(**config_dict)
    except tomli.TOMLDecodeError as e:
        exit_on_error(f"Failed to parse TOML file: {e}")
    except FileNotFoundError:
        exit_on_error(f"File not found: {config_file_path}")

    asyncio.run(main(config))