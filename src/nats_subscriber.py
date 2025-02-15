import asyncio
from nats.aio.client import Client as NATS
from config import CONFIG  

class NatsSubscriber:
    def __init__(self):
        self.nats_url = CONFIG["NATS_URL"]
        self.subject = CONFIG["SUBJECT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.nc = NATS()
        self.stop_event = asyncio.Event()
        self.messages = []  # used for handling ordered messages

    async def connect(self):
        print(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        print("Connected to NATS!")

    async def subscribe(self):
        async def message_handler(msg):
            raw_message = msg.data.decode()
            print(f"Received from NATS: {raw_message}")

            if self.is_loose_order:
                self.messages.append(raw_message)
                if len(self.messages) > 1:
                    self.messages.sort()  # sort ordered messages
                while self.messages:
                    m = self.messages.pop(0)
                    print(f"Processed ordered message: {m}")
            else:
                print(f"Processed message: {raw_message}")

        print(f"Subscribing to subject: {self.subject}")
        await self.nc.subscribe(self.subject, cb=message_handler)

async def main():
    subscriber = NatsSubscriber()
    await subscriber.connect()
    await subscriber.subscribe()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
