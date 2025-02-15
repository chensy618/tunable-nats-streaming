import asyncio
from nats.aio.client import Client as NATS
import signal
import sys

class NatsSubscriber:
    def __init__(self, nats_url: str, subject: str):
        self.nats_url = nats_url
        self.subject = subject
        self.nc = NATS()
        self.stop_event = asyncio.Event()  # 用于保持运行状态

    async def connect(self):
        print(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        print("Connected to NATS!")

    async def subscribe(self):
        async def message_handler(msg):
            print(f"Received message: {msg.data.decode()}")

        print(f"Subscribing to subject: {self.subject}")
        await self.nc.subscribe(self.subject, cb=message_handler)

    async def close(self):
        print("Closing connection to NATS...")
        await self.nc.close()
        print("Connection closed.")

    async def run(self):
        await self.connect()
        await self.subscribe()
        print("Listening for messages... Press Ctrl+C to exit.")
        await self.stop_event.wait()  # 阻塞，保持监听状态

    def stop(self):
        self.stop_event.set()  # 终止阻塞，关闭监听

# 捕获 Ctrl + C，优雅退出
def signal_handler(sig, frame):
    print("\nReceived termination signal. Shutting down...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# 启动订阅者
async def main():
    subscriber = NatsSubscriber(nats_url="nats://localhost:4222", subject="test.subject")
    try:
        await subscriber.run()
    except asyncio.CancelledError:
        print("Subscriber cancelled.")

if __name__ == "__main__":
    asyncio.run(main())
