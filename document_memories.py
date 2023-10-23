from pydantic import BaseModel, ValidationError, Field
from typing import List
from enum import Enum
from aio_pika.exceptions import ProbableAuthenticationError, AMQPConnectionError, ChannelNotFoundEntity, ChannelClosed
from aiormq.exceptions import ChannelAccessRefused
from aio_pika import Connection
from espa import exit_on_error, File
import tomli, asyncio, sys, aio_pika, chromadb, fitz, json

class DatabaseType(int):
    IN_MEMORY = 1
    ON_FILE = 2

class DocumentMemoriesConfig(BaseModel):
    host: str
    user: str
    password: str
    assimilate_file_mq: str
    memories_exchange: str
    memories_routing_key: str
    database_type: DatabaseType
    
chroma_client = chromadb.Client()
collection = chroma_client.create_collection(name="vector_memories")

async def main(config: DocumentMemoriesConfig):
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
        channel = await connection.channel()
        queue = await channel.get_queue(config.assimilate_file_mq)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    try:
                        data = File(**json.loads(message.body.decode()))
                    except json.JSONDecodeError:
                        print(f"Error: Failed to decode JSON in message body, Data: {message.body}")
                        continue
                    except ValidationError as validation_error:
                        print(f"Validation error: {str(validation_error)}")
                        continue
                    await consume_file(data)
                
        print("Finished executing.", file=sys.stderr)
    except asyncio.CancelledError:
        print("Execution was cancelled prematurely.", file=sys.stderr)

async def consume_file(file: File):
    print(file.file_name)
    # try to read the file
    # save general metadata
    # batch content for storage
    # store every batch
    # report success on exchange
    pass

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