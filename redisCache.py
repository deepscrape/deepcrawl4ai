# for async use
import os
from typing import List, Optional
from dotenv import load_dotenv
# from redis.asyncio import Redis
from upstash_ratelimit.asyncio import Ratelimit, FixedWindow, TokenBucket
from upstash_redis.asyncio import Redis
from redis.asyncio import Redis as PureRedis
import asyncio

REDIS_CHANNEL = "stream_channel"  # Default channel for streaming data

# ────────────────── configuration  ──────────────────
production = os.getenv("PYTHON_ENV", "development").lower() == "production" 
env_file = ".env" if production else "dev.env"

# Load environment variables
load_dotenv(env_file, verbose=True)

redis_url = os.environ.get("UPSTASH_REDIS_REST_URL")
redis_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

REDIS_PORT = os.environ.get("UPSTASH_REDIS_PORT")
REDIS_USERNAME = os.environ.get("UPSTASH_REDIS_USER")
REDIS_PASSWORD = os.environ.get("UPSTASH_REDIS_PASS")

if not redis_url or not redis_token or not REDIS_PORT or not REDIS_USERNAME or not REDIS_PASSWORD:
    raise ValueError("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN environment variables must be set")

REDIS_URL = redis_url.replace("https://", "")

# ────────────────── redis client  ──────────────────
# Initialize Redis client with Upstash credentials
redis = Redis(
    url=redis_url,
    token=redis_token,
    allow_telemetry=False,  # Disable telemetry if not needed
)

# Initialize PureRedis client for pub/sub operations
pure_redis = PureRedis(
    host=REDIS_URL,  # Replace with your Redis server's hostname or IP
    port=int(REDIS_PORT),         # Default Redis port
    db=0,              # Default database number
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    ssl=True,  # Use SSL if your Redis server supports it
    decode_responses=True,  # Optional: decode responses to UTF-8 strings
)

async def test_connection(redis: Redis | PureRedis):
    retries = 0
    max_retries = 100
    retry_delay = 12  # seconds

    while retries < max_retries:
        try:
            await redis.ping()
            print("\033[94mINFO-DB:\033[0m  \033[92mRedis connected successfully!\033[0m")
            return
        except Exception as e:
            retries += 1
            print(f"\033[91mERROR-DB:\033[0m Redis connection failed: {e}")
            print(f"\033[93mWARNING-DB:\033[0m Trying again in 12.00 seconds... (attempt {retries}/{max_retries})")
            if retries < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                print("\033[91mERROR-DB:\033[0m Max retries reached. Could not connect to Redis.")
                return


# ─────────────────── rate limiters  ──────────────────
# Default rate limiter: 10 requests per 10 seconds
default_limiter = Ratelimit(
    redis=redis,
    limiter=TokenBucket(max_tokens=10, refill_rate=5, interval=10),
    prefix="@upstash/ratelimit",
)

# Custom rate limiter: 5 requests per 60 seconds
custom_limiter = Ratelimit(
    redis=redis,
    limiter=FixedWindow(max_requests=1, window=60),
    prefix="@upstash/ratelimit"
)

# ─────────────────── asyncio test  ──────────────────
# asyncio.run(test_connection(pure_redis))


# ─────────────────── redis execute  ──────────────────
async def redis_execute(redis: Redis, command: List, *args):
    """Execute a Redis command and handle errors."""
    if not redis:
        print("\033[91mERROR-DB:\033[0m Redis client is not initialized.")
        return None
    if not command:
        print("\033[91mERROR-DB:\033[0m Command is empty.")
        return None
    
    try:
        result = await redis.execute(command, *args)
        return result
    except Exception as e:
        print(f"\033[91mERROR-DB:\033[0m Redis command '{command}' failed: {e}")
        return None
    

async def redis_subscribe(redis: Redis, channel: str):
    """Subscribe to a Redis channel."""
    if not redis:
        print("\033[91mERROR-DB:\033[0m Redis client is not initialized.")
        return None
    if not channel:
        print("\033[91mERROR-DB:\033[0m Channel is empty.")
        return None
    redis.publish

    try:
        await redis_execute(redis, ["SUBSCRIBE", channel])
        print(f"\033[94mINFO-DB:\033[0m  \033[92mSubscribed to channel '{channel}'\033[0m")
    except Exception as e:
        print(f"\033[91mERROR-DB:\033[0m Failed to subscribe to channel '{channel}': {e}")
        return None
    

async def redis_xadd(pipe, channel: str, message: dict, maxlen: Optional[int] = None, approximate: bool = False):
    """Add a message to a Redis stream with optional maxlen."""
    if not pipe:
        print("\033[91mERROR-DB:\033[0m Redis pipe client is not initialized.")
        return None
    if not channel:
        print("\033[91mERROR-DB:\033[0m Channel is empty.")
        return None
    if not message:
        print("\033[91mERROR-DB:\033[0m Message is empty.")
        return None

    try:
        pieces = [channel]
        if maxlen is not None:
            if maxlen < 0:
                raise ValueError("maxlen must be a non-negative integer")
            pieces.append("MAXLEN")
            if approximate:
                pieces.append("~")
            pieces.append(str(maxlen))
        for key, value in message.items():
            pieces.extend([key, value])

        # Upstash client exposes `execute`; redis-py uses `execute_command` or `xadd`
        if hasattr(pipe, "execute"):
            message_id = await pipe.execute("XADD", *pieces)
        elif hasattr(pipe, "xadd"):
            kwargs = {}
            if maxlen is not None:
                kwargs["maxlen"] = maxlen
                kwargs["approximate"] = approximate
            message_id = await pipe.xadd(channel, message, **kwargs)
        else:
            # Fallback to redis-py low-level
            message_id = await pipe.execute_command("XADD", *pieces)
        return message_id
    except Exception as e:
        print(f"\033[91mERROR-DB:\033[0m Failed to add message to stream '{channel}': {e}")
        return None
    

async def redis_xread(redis: Redis, streams: dict, count: Optional[int] = None, block: Optional[int] = None):
    """
    Read messages from a Redis stream using XREAD.
    :param redis: Redis client (PureRedis)
    :param streams: Dictionary of {channel: last_id}
    :param count: Maximum number of entries to return
    :param block: Number of milliseconds to block if no messages are available
    :return: List of messages or None
    """
    if not redis:
        print("\033[91mERROR-DB:\033[0m Redis redis client is not initialized.")
        return None
    if not streams:
        print("\033[91mERROR-DB:\033[0m Streams dictionary is empty.")
        return None

    try:
        # Build the XREAD command as a list
        command = ["XREAD"]
        if count is not None:
            command.extend(["COUNT", str(count)])
        if block is not None:
            command.extend(["BLOCK", str(block)])
        command.append("STREAMS")
        for channel, last_id in streams.items():
            command.append(channel)
        for channel, last_id in streams.items():
            command.append(last_id)
        result = await redis.execute(command)
        return result
    except Exception as e:
        print(f"\033[91mERROR-DB:\033[0m Failed to read from stream(s) '{list(streams.keys())}': {e}")
        return None
# ─────────────────── redis pub/sub  ──────────────────
