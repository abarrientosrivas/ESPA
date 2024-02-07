from pydantic import BaseModel, ValidationError, Field
from typing import List
from enum import Enum
from aio_pika.exceptions import ProbableAuthenticationError, AMQPConnectionError, ChannelNotFoundEntity, ChannelClosed
from aiormq.exceptions import ChannelAccessRefused
from aio_pika import Connection
from espa import exit_on_error, File, Memory
import tomli, asyncio, sys, aio_pika, chromadb, fitz, json, re, uuid, datetime, os

class DocumentMemoriesConfig(BaseModel):
    host: str
    user: str
    password: str
    assimilate_file_mq: str
    memories_exchange: str
    memories_routing_key: str
    database_file_path: str
    persistent_database: bool
    collection_name: str

async def main(config: DocumentMemoriesConfig):
    try:
        try:
            if (config.persistent_database):
                chroma_client = chromadb.PersistentClient(path=config.database_file_path)
            else:
                chroma_client = chromadb.Client()
            chroma_client.heartbeat()
            collection = chroma_client.get_or_create_collection(name=config.collection_name)
        except:
            exit_on_error("Could not initialize database.")
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
        consumer_channel = await connection.channel()
        publisher_channel = await connection.channel()
        queue = await consumer_channel.get_queue(config.assimilate_file_mq)
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
                    exchange = await publisher_channel.get_exchange(config.memories_exchange)
                    await consume_file(data, collection, exchange, config)
                
        print("Finished executing.", file=sys.stderr)
    except asyncio.CancelledError:
        print("Execution was cancelled prematurely.", file=sys.stderr)

async def consume_file(file: File, collection, exchange, routing_key: str):
    if not os.path.exists(file.file_path):
        print(f"Target file at '{file.file_path}' not found, skipping.")
        return

    print(f"Extracting memories from file {file.file_name}.", file=sys.stderr)
    batches = remove_non_ascii(split_into_paragraphs(GetFileTextContent(file.file_path)))

    for batch in batches:
        id = str(uuid.uuid4())
        collection.add(
            documents=batch,
            metadatas={"source": file.file_path},
            ids=id
        )

        await exchange.publish(
            aio_pika.Message(body=Memory(created_at=datetime.utcnow(), content=batch, id=id).model_dump_json().encode()),
            routing_key=routing_key
        )

    print(f"Memories from file {file.file_name} extracted.", file=sys.stderr)

def GetFileTextContent(filename: str) -> str:
    doc = fitz.open(filename)
    text_content = []
    for page_num in range(len(doc)):
        page = doc[page_num]
        text_content.append(page.get_text())
    return "\n".join(text_content)

def remove_non_ascii(strings: List[str]) -> List[str]:
    cleaned_strings: List[str] = []
    for string in strings:
        # Keeping only ASCII characters from 32 to 126
        cleaned_string = re.sub(r'[^\x20-\x7E\u00C0-\u00FF]', '', string)
        if cleaned_string and not cleaned_string.isspace():
            cleaned_strings.append(cleaned_string)
    return cleaned_strings

def split_into_paragraphs(text: str) -> List[str]:
    paragraphs = text.split('.\n')
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    return paragraphs

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