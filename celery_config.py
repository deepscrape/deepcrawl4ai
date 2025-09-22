# celeryconfig.py - Celery configuration file

# Broker settings (LavinMQ)

# Replace BROKER_URL with your LavinMQ server's URL
# Example : 'amqp://guest:guest@localhost:5672//'
# Default password and username for lavinmq user: guest, pass: guest

BROKER_URL = 'lavinmq://<username>:<password>@<lavinmq_host>:<lavinmq_port>/<virtual_host>'

# Result backend (Optional, if you want to store task results)
# result_backend = 'rpc://

# Recommended settings for local LavinMQ
broker_pool_limit = 1
broker_heartbeat = None
broker_connection_timeout = 30
result_backend = None
event_queue_expires = 60
worker_prefetch_multiplier = 1
worker_concurrency = 4 # Adjust based on your system's capabilities