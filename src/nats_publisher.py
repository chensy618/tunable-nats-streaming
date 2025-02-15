import asyncio
import socket

UDP_IP = "127.0.0.1"  # Change this to match your UDP relay's address
UDP_PORT = 5005       # UDP port for transmission

async def udp_publish(message: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(message.encode(), (UDP_IP, UDP_PORT))
    print(f"UDP Message sent: {message}")

if __name__ == "__main__":
    asyncio.run(udp_publish("Hello via UDP!"))
