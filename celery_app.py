import logging
import os
from celery import Celery
from dotenv import load_dotenv


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
)

if __name__ == "__main__":
    celery_app.start()
