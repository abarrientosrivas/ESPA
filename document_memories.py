from pydantic import BaseModel, ValidationError, Field
from typing import List
from enum import Enum
from aio_pika.exceptions import ProbableAuthenticationError, AMQPConnectionError, ChannelNotFoundEntity, ChannelClosed
from aiormq.exceptions import ChannelAccessRefused
from aio_pika import Connection
from espa import exit_on_error, File
import tomli, asyncio, sys, aio_pika, chromadb, fitz, json, re, uuid

class DocumentMemoriesConfig(BaseModel):
    host: str
    user: str
    password: str
    assimilate_file_mq: str
    memories_exchange: str
    memories_routing_key: str
    persistent_database: bool

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
    print(f"Extracting memories from file {file.file_name}.", file=sys.stderr)
    batches = remove_non_ascii(split_into_paragraphs(GetFileTextContent(file.file_path)))

    collection.add(
        documents=batches,
        metadatas=[{"source": file.file_path}] * len(batches),
        ids=uuid_list(len(batches))
    )
    
    # publish on the "memories_exchange" with the routing key "memories_routing_key"

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
    
    if (config.persistent_database):
        chroma_client = chromadb.PersistentClient(path="/path/to/save/to")
    else:
        chroma_client = chromadb.Client()
    chroma_client.heartbeat()
    collection = chroma_client.get_or_create_collection(name="vector_memories")
    
    asyncio.run(main(config))

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

def uuid_list(size: int) -> List[str]:
    if size <= 0:
        return []

    uuids = [str(uuid.uuid4()) for _ in range(size)]
    return uuids