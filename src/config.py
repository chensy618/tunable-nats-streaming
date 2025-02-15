# config.py - global configuration file
CONFIG = {
    "UDP_IP": "127.0.0.1",
    "UDP_PORT": 5005,
    "NATS_URL": "nats://localhost:4222",
    "SUBJECT": "tundra.subject",
    "IS_LOOSE_ORDER": True,  # enable loose ordering of messages
    "IS_PARTIAL_RELIABLE": True  # enable partial reliability
}
