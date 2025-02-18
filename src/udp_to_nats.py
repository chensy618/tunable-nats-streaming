import asyncio
import socket
import random
import signal
import threading
from nats.aio.client import Client as NATS
from config import CONFIG  

class UdpToNats:
    def __init__(self):
        self.nats_url = CONFIG["NATS_URL"]
        self.subject = CONFIG["SUBJECT"]
        self.udp_ip = CONFIG["UDP_IP"]
        self.udp_port = CONFIG["UDP_PORT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.is_partial_reliable = CONFIG["IS_PARTIAL_RELIABLE"]
        self.nc = NATS()
        self.messages = []  # Store unordered messages
        self.running = True  # Control main loop
        self.sock = None  # Save UDP socket instance

    async def connect_nats(self):
        print(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        print("Connected to NATS!")

    async def handle_udp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.udp_ip, self.udp_port))
        print(f"Listening for UDP messages on {self.udp_ip}:{self.udp_port}...")

        while self.running:
            try:
                self.sock.settimeout(1.0)  # Set timeout to prevent blocking
                data, addr = self.sock.recvfrom(1024)
                if not self.running:  # Stop processing after socket is closed
                    break

                raw_message = data.decode()
                print(f"Received UDP message from {addr}: {raw_message}")

                try:
                    is_loose_order, is_partial_reliable, message = raw_message.split("|", 2)
                    is_loose_order = is_loose_order.lower() == "true"
                    is_partial_reliable = is_partial_reliable.lower() == "true"
                except ValueError:
                    print("Invalid message format, discarding...")
                    continue
                
                # Handle partial reliability (randomly drop 10% of messages)
                if self.is_partial_reliable and is_partial_reliable and random.random() < 0.1:
                    print(f"Dropped message due to partial reliability: {message}")
                    continue

                # Handle loose ordered messages, store the messages in buffer, 
                # and then shuffle them to simulate unordered characteristics
                if self.is_loose_order and is_loose_order:
                    self.messages.append(message)
                    if len(self.messages) > 1:
                        random.shuffle(self.messages)  # Shuffle unordered messages
                    while self.messages:
                        msg = self.messages.pop(0)
                        await self.nc.publish(self.subject, msg.encode())
                        await self.nc.flush()
                        print(f"Published ordered message to NATS: {msg}")
                else:
                    await self.nc.publish(self.subject, message.encode())
                    await self.nc.flush()
                    print(f"Published to NATS: {message}")

            except socket.timeout:
                continue  # Continue checking self.running after timeout
            except OSError:
                print("Socket closed, exiting UDP loop.")
                break

    async def stop(self):
        """Stop the NATS connection and exit"""
        print("\nStopping UDP to NATS relay...")
        self.running = False  # Exit main loop
        if self.sock:
            try:
                self.sock.close()  # Close UDP socket
            except OSError:
                pass  # Avoid errors if socket is closed multiple times
        await self.nc.close()  # Close NATS connection
        print("UDP to NATS relay stopped.")

def exit_listener(relay):
    """Listen for 'exit' command to terminate the program"""
    while relay.running:
        cmd = input().strip().lower()
        if cmd == "exit":
            print("Received exit command. Stopping...")
            asyncio.run(relay.stop())
            break

async def main():
    relay = UdpToNats()
    await relay.connect_nats()

    # Handle Ctrl+C to exit
    def handle_ctrl_c():
        print("\nReceived Ctrl+C. Stopping UDP to NATS relay...")
        asyncio.run(relay.stop())

    signal.signal(signal.SIGINT, lambda sig, frame: handle_ctrl_c())

    # Start `exit` listener thread
    exit_thread = threading.Thread(target=exit_listener, args=(relay,))
    exit_thread.start()

    # Run UDP listener
    await relay.handle_udp()

if __name__ == "__main__":
    asyncio.run(main())
