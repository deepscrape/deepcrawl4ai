# for async use
import os
from upstash_redis.asyncio import Redis

# if you want to automatically load the credentials from the environment
# redis = Redis.from_env()

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"), token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)
