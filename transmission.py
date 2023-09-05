import tomli, asyncio
from aio_pika import connect, Message, DeliveryMode

with open("MQ_Topology.toml", "rb") as f:
    config = tomli.load(f)

async def main():
    host = config['message_broker']['host']
    user = config['message_broker']['user']
    password = config['message_broker']['password']
    connection = await connect(
        f"amqp://{user}:{password}@{host}/"
    )
    channel = await connection.channel()
    print(channel)
    

if __name__ == "__main__":
    asyncio.run(main())