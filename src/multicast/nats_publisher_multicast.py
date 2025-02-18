import asyncio
import socket
from config import CONFIG

class NatsMulticastPublisher:
    def __init__(self):
        # use multicast to send UDP messages
        self.udp_ip = CONFIG["UPD_MULTICAST_IP"]
        self.udp_port = CONFIG["UDP_PORT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.is_partial_reliable = CONFIG["IS_PARTIAL_RELIABLE"]
        self.sequence_number = 0  # sequence number for messages

    async def publish(self, message: str):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # set multicast TTL
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, CONFIG["MULTICAST_TTL"])
        self.sequence_number += 1
        # format：seq_num|is_looser_order|is_partial_reliable|content
        formatted_msg = f"{self.sequence_number}|{self.is_loose_order}|{self.is_partial_reliable}|{message}"
        sock.sendto(formatted_msg.encode(), (self.udp_ip, self.udp_port))
        print(f"UDP Multicast sent: {formatted_msg}")

async def main():
    publisher = NatsMulticastPublisher()
    
    user_choice = input("\nWould you like to enter custom messages? (yes/no): ").strip().lower()
    if user_choice == "yes":
        print("Enter messages to send. Type 'exit' to quit.")
        while True:
            message = input("Enter message: ").strip()
            if message.lower() == "exit":
                print("Exiting publisher...")
                break
            await publisher.publish(message)
    else:
        await publisher.publish("Hello via UDP Multicast!")

if __name__ == "__main__":
    asyncio.run(main())
