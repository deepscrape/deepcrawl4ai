import asyncio
import logging
import sys
from kombu import Queue
import os
from celery import Celery
from dotenv import load_dotenv
import signal


# if sys.platform == "win32":
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# else:
#     import uvloop  # type: ignore
#     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    
# ────────────────── configuration  ──────────────────
load_dotenv(verbose=True)

# Celery needs a URI for broker and backend, even if we're passing an instance.
# For Upstash Redis, the URL is typically what's needed.
# We'll use the URL from redisCache.py's environment variables.UPSTASH_REDIS_REST_PASSWORD
#  rediss://default:4c0962711bf64ff8b7797d38dc0e69e5@gusc1-saved-terrapin-30766.upstash.io:30766/0?ssl_cert_reqs=CERT_REQUIRED
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_PORT = os.environ.get("UPSTASH_REDIS_PORT")
REDIS_USERNAME = os.environ.get("UPSTASH_REDIS_USER")
REDIS_PASSWORD = os.environ.get("UPSTASH_REDIS_PASS")


if not REDIS_URL or not REDIS_PORT or not REDIS_PASSWORD or not REDIS_USERNAME:
    raise ValueError("UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_PORT, UPSTASH_REDIS_USER, UPSTASH_REDIS_REST_PASSWORD environment variables must be set for Celery configuration.")

REDIS_URL = REDIS_URL.replace("https://", "")

REDIS_URI = f"rediss://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_URL}:{REDIS_PORT}/0?ssl_cert_reqs=CERT_REQUIRED"

celery_app = Celery(
    "deepcrawl4ai",
    broker=REDIS_URI,
    backend=REDIS_URI,
    include=["tasks"]  # We will create a tasks.py later
)

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    # task_soft_time_limit=600,  # Soft time limit: 10 minutes
    # task_time_limit=900,      # Hard time limit: 15 minutes
    # result_expires=3600,      # Expire results after 1 hour to avoid memory bloat
)

def graceful_shutdown(signum, frame):
    logging.info(f"Received signal {signum}, shutting down Celery worker gracefully...")
    from celery.worker import state
    state.should_stop = True
    # Optionally: wait for tasks to finish, cleanup resources

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)
    
# celery_app.conf.task_queues = (
#     Queue("light_jobs"),
#     Queue("heavy_jobs"),
# )
# celery_app.conf.task_default_queue = "light_jobs"  # fallback if not specified

if __name__ == "__main__":
    celery_app.start()
