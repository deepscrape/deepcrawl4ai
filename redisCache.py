# for async use
import asyncio
import os
from typing import List
from dotenv import load_dotenv
# from redis.asyncio import Redis
from upstash_ratelimit.asyncio import Ratelimit, FixedWindow, TokenBucket
from upstash_redis.asyncio import Redis
from redis.asyncio import Redis as PureRedis

REDIS_CHANNEL = "stream_channel"  # Default channel for streaming data

# ────────────────── configuration  ──────────────────
load_dotenv()

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
    decode_responses=True  # Optional: decode responses to UTF-8 strings
)

async def test_connection(redis: Redis | PureRedis):
    try:
        await redis.ping()
        
        print("\033[94mINFO-DB:\033[0m  \033[92mRedis connected successfully!\033[0m")
    except Exception as e:
        print(f"\033[91mERROR-DB:\033[0m Redis connection failed: {e}")


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
    