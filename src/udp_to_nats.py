import asyncio
import socket
import signal
import sys
from nats.aio.client import Client as NATS

UDP_IP = "127.0.0.1"
UDP_PORT = 5005
BUFFER_SIZE = 1024

class UDPToNATS:
    def __init__(self, nats_url: str, subject: str):
        self.nats_url = nats_url
        self.subject = subject
        self.nc = NATS()
        self.running = True  # 添加退出控制

    async def connect_nats(self):
        print(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        print("Connected to NATS!")

    async def handle_udp(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((UDP_IP, UDP_PORT))
        print(f"Listening for UDP messages on {UDP_IP}:{UDP_PORT}...")

        while self.running:  # 只有 running 为 True 才继续循环
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
                message = data.decode()
                print(f"Received UDP message from {addr}: {message}")
                await self.nc.publish(self.subject, message.encode())
                await self.nc.flush()
            except KeyboardInterrupt:
                print("Shutdown requested, exiting...")
                self.running = False
                break  # 退出循环

    def stop(self):
        self.running = False  # 外部信号可以修改 running 状态

def signal_handler(sig, frame):
    print("Received termination signal, stopping...")
    sys.exit(0)

# 捕获 Ctrl + C
signal.signal(signal.SIGINT, signal_handler)

async def main():
    relay = UDPToNATS(nats_url="nats://localhost:4222", subject="test.subject")
    await relay.connect_nats()
    await relay.handle_udp()

if __name__ == "__main__":
    asyncio.run(main())
