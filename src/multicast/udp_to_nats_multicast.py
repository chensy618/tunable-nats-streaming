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
        # 使用配置中的多播地址接收数据
        self.udp_multicast_ip = CONFIG["UPD_MULTICAST_IP"]
        self.udp_port = CONFIG["UDP_PORT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.is_partial_reliable = CONFIG["IS_PARTIAL_RELIABLE"]
        self.nc = NATS()
        # 使用字典缓存乱序消息，key 为序列号，value 为消息内容
        self.buffer = {}
        self.expected_sequence = 1
        self.running = True
        self.sock = None

    async def connect_nats(self):
        print(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        print("Connected to NATS!")

    async def handle_udp(self):
        # 创建 UDP 多播 socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 绑定到所有接口的指定端口
        self.sock.bind(('', self.udp_port))
        # 加入多播组
        mreq = socket.inet_aton(self.udp_multicast_ip) + socket.inet_aton("0.0.0.0")
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        print(f"Listening for UDP multicast on {self.udp_multicast_ip}:{self.udp_port}...")

        while self.running:
            try:
                self.sock.settimeout(1.0)
                data, addr = self.sock.recvfrom(1024)
                if not self.running:
                    break

                raw_message = data.decode()
                print(f"Received UDP message from {addr}: {raw_message}")

                # 消息格式：seq_num|is_loose_order|is_partial_reliable|message
                parts = raw_message.split("|", 3)
                if len(parts) != 4:
                    print("Invalid message format, discarding...")
                    continue

                seq_num_str, is_loose_order_str, is_partial_reliable_str, message = parts
                try:
                    seq_num = int(seq_num_str)
                except ValueError:
                    print("Invalid sequence number, discarding message.")
                    continue

                is_loose_order = is_loose_order_str.lower() == "true"
                is_partial_reliable = is_partial_reliable_str.lower() == "true"

                # 对于部分可靠性：若启用且消息标记为部分可靠，则有 10% 的概率丢弃消息
                if self.is_partial_reliable and is_partial_reliable and random.random() < 0.1:
                    print(f"Dropped message due to partial reliability: seq {seq_num}, message: {message}")
                    continue

                if self.is_loose_order and is_loose_order:
                    # 如果接收到的序列号正好是预期的，则立即发布
                    if seq_num == self.expected_sequence:
                        await self._publish_to_nats(message)
                        self.expected_sequence += 1
                        # 检查缓存中是否存在连续的后续消息
                        while self.expected_sequence in self.buffer:
                            buffered_message = self.buffer.pop(self.expected_sequence)
                            await self._publish_to_nats(buffered_message)
                            self.expected_sequence += 1
                    else:
                        # 缓存乱序消息
                        self.buffer[seq_num] = message
                        print(f"Buffered out-of-order packet: seq {seq_num}")
                else:
                    await self._publish_to_nats(message)

            except socket.timeout:
                continue
            except OSError:
                print("Socket closed, exiting UDP loop.")
                break

    async def _publish_to_nats(self, message):
        await self.nc.publish(self.subject, message.encode())
        await self.nc.flush()
        print(f"Published to NATS: {message}")

    async def stop(self):
        print("\nStopping UDP to NATS relay...")
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        await self.nc.close()
        print("UDP to NATS relay stopped.")

def exit_listener(relay):
    while relay.running:
        cmd = input().strip().lower()
        if cmd == "exit":
            print("Received exit command. Stopping...")
            asyncio.run(relay.stop())
            break

async def main():
    relay = UdpToNats()
    await relay.connect_nats()

    # 处理 Ctrl+C 信号
    def handle_ctrl_c():
        print("\nReceived Ctrl+C. Stopping UDP to NATS relay...")
        asyncio.run(relay.stop())

    signal.signal(signal.SIGINT, lambda sig, frame: handle_ctrl_c())

    exit_thread = threading.Thread(target=exit_listener, args=(relay,))
    exit_thread.start()

    await relay.handle_udp()

if __name__ == "__main__":
    asyncio.run(main())
