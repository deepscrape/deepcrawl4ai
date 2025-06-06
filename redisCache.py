# for async use
import asyncio
import os
from dotenv import load_dotenv
# from redis.asyncio import Redis
from upstash_ratelimit.asyncio import Ratelimit, FixedWindow, TokenBucket
from upstash_redis.asyncio import Redis

# ────────────────── configuration  ──────────────────
load_dotenv()

redis_url = os.environ.get("UPSTASH_REDIS_REST_URL")
redis_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not redis_url or not redis_token:
    raise ValueError("UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN environment variables must be set")

redis = Redis(
    url=redis_url,
    token=redis_token,
)

async def test_connection(redis: Redis):
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
asyncio.run(test_connection(redis))