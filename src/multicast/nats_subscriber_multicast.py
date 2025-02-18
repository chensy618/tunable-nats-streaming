import asyncio
import logging
from nats.aio.client import Client as NATS
from config import CONFIG  

class NatsSubscriber:
    def __init__(self):
        self.nats_url = CONFIG["NATS_URL"]
        self.subject = CONFIG["SUBJECT"]
        self.is_loose_order = CONFIG["IS_LOOSE_ORDER"]
        self.is_partial_reliable = CONFIG["IS_PARTIAL_RELIABLE"]
        self.nc = NATS()
        self.stop_event = asyncio.Event()
        self.last_seq_num = 0  # 用于跟踪最后处理的序列号
        # 配置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    async def connect(self):
        logging.info(f"Connecting to NATS at {self.nats_url}...")
        await self.nc.connect(servers=[self.nats_url])
        logging.info("Connected to NATS!")

    async def subscribe(self):
        async def message_handler(msg):
            try:
                raw_message = msg.data.decode()
                logging.info(f"Received from NATS: {raw_message}")
                
                # 假设消息格式为 "seq_num|message_content"，如果消息中有序列号
                parts = raw_message.split("|", 1)
                if len(parts) == 2:
                    seq_num_str, content = parts
                    try:
                        seq_num = int(seq_num_str)
                        # 检查消息顺序（如果有缺失或乱序，可做报警或重试逻辑）
                        if seq_num != self.last_seq_num + 1:
                            logging.warning(f"消息乱序或缺失：上次 {self.last_seq_num}，本次 {seq_num}")
                        self.last_seq_num = seq_num
                    except ValueError:
                        content = raw_message  # 如果解析失败，则认为消息没有序列号
                else:
                    content = raw_message

                # 根据内容进行业务逻辑分流，例如区分命令与状态更新
                if content.startswith("CMD:"):
                    # 处理命令消息
                    logging.info(f"Processing command: {content}")
                    # 在这里加入相应的业务逻辑处理
                else:
                    # 处理普通消息
                    logging.info(f"Processing data message: {content}")
                    # 这里可以加入去重、过滤、数据转换等业务逻辑

                # 最终处理完毕后的操作
                logging.info(f"Processed message: {raw_message}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                # 可选择将错误消息写入重试队列，或者执行其他补救措施

        logging.info(f"Subscribing to subject: {self.subject}")
        await self.nc.subscribe(self.subject, cb=message_handler)

async def main():
    subscriber = NatsSubscriber()
    await subscriber.connect()
    await subscriber.subscribe()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
