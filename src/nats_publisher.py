import asyncio
import socket
from config import CONFIG  # import configuration

class NatsPublisher:
    def __init__(self):
        self.udp_ip = CONFIG["UDP_IP"]
        self.udp_port = CONFIG["UDP_PORT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.is_partial_reliable = CONFIG["IS_PARTIAL_RELIABLE"]

    async def publish(self, message: str):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        formatted_msg = f"{self.is_loose_order}|{self.is_partial_reliable}|{message}"
        sock.sendto(formatted_msg.encode(), (self.udp_ip, self.udp_port))
        print(f"UDP Message sent: {formatted_msg}")

async def main():
    publisher = NatsPublisher()
    
    # Prompt user to enter custom messages
    user_choice = input("\n Would you like to enter custom messages? (yes/no): ").strip().lower()
    if user_choice == "yes":
        print(" Enter messages to send. Type 'exit' to quit.")
        while True:
            message = input("Enter message: ").strip()
            if message.lower() == "exit":
                print("Exiting publisher...")
                break
            await publisher.publish(message)
    else:
        # Send a default message
        await publisher.publish("Hello via UDP!")

if __name__ == "__main__":
    asyncio.run(main())
