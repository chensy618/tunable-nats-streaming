# config.py - global configuration file
CONFIG = {
    "UDP_IP": "127.0.0.1",
    "UPD_MULTICAST_IP": "239.255.0.1",
    "UDP_PORT": 5005,
    "TCP_IP": "0.0.0.0", # if needed, add a layer of security by specifying the TCP server
    "TCP_PORT": 5006,
    "NATS_URL": "nats://localhost:4222",
    "SUBJECT": "tundra.subject",
    "IS_LOOSE_ORDER": True,  # enable loose ordering of messages
    "IS_PARTIAL_RELIABLE": True,  # enable partial reliability
    "MULTICAST_TTL": 2
}
