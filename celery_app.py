import logging
import sys
# from kombu import Queue
import os
from celery import Celery
from dotenv import load_dotenv
import signal
from urllib.parse import urlparse

# if sys.platform == "win32":
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# else:
#     import uvloop  # type: ignore
#     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
production = os.getenv("PYTHON_ENV", "development").lower() == "production" 
env_file = ".env" if production else "dev.env"

# Load environment variables
load_dotenv(env_file, verbose=True)

# Celery needs a URI for broker and backend, even if we're passing an instance.
# For Upstash Redis, the URL is typically what's needed.
# We'll use the URL from redisCache.py's environment variables.UPSTASH_REDIS_REST_PASSWORD
#  rediss://default:4c0962711bf64ff8b7797d38dc0e69e5@gusc1-saved-terrapin-30766.upstash.io:30766/0?ssl_cert_reqs=CERT_REQUIRED
redis_url = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_PORT = os.environ.get("UPSTASH_REDIS_PORT")
REDIS_USERNAME = os.environ.get("UPSTASH_REDIS_USER")
REDIS_PASSWORD = os.environ.get("UPSTASH_REDIS_PASS")


if not redis_url or not REDIS_PORT or not REDIS_PASSWORD or not REDIS_USERNAME:
    raise ValueError("UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_PORT, UPSTASH_REDIS_USER, UPSTASH_REDIS_PASS environment variables must be set for Celery configuration.")

REDIS_URL = urlparse(redis_url).hostname or redis_url.replace("https://", "").replace("http://", "")

REDIS_URI = f"rediss://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_URL}:{REDIS_PORT}/0?ssl_cert_reqs=CERT_REQUIRED"

celery_app = Celery(
    "crawlagent",
    broker=REDIS_URI,
    backend=REDIS_URI,
    include=["tasks"],  # We will create a tasks.py later
)
# celery_app.config_from_object('celeryconfig')

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    # broker_pool_limit = 2,  # Default is 10
    # broker_connection_max_retries = 3, # Default is 100
    # broker_heartbeat = 0,  # Disabled by default
    # broker_transport_options = {'visibility_timeout': 3600},  # 1 hour
    timezone='UTC',
    enable_utc=True,
    task_soft_time_limit=600,  # Soft time limit: 10 minutes
    task_time_limit=900,      # Hard time limit: 15 minutes
    result_expires=3600,      # Expire results after 1 hour to avoid memory bloat
    # broker_connection_retry=True,
    # broker_connection_retry_on_startup=True,
    # broker_connection_max_retries=10,
    # broker_connection_timeout=30,
    # broker_transport_options={
    #     'visibility_timeout': 3600,
    #     'socket_timeout': 30,
    #     'socket_connect_timeout': 30,
    # },
    # redis_socket_keepalive=True,

    # Windows-specific settings
    worker_cancel_long_running_tasks_on_connection_loss=True,  # Helps with Windows task cancellation
    task_remote_tracebacks=True,  # Better error reporting
    worker_max_tasks_per_child=1 if os.name == 'nt' else None,  # Prevent memory leaks on Windows
)

# Add Windows-specific signal handling
if os.name == 'nt':
    # Windows doesn't support SIGKILL/SIGTERM the same way
    def windows_shutdown_handler(*args):
        print("Windows shutdown signal received")
        celery_app.control.broadcast('shutdown')
        sys.exit(0)

    signal.signal(signal.SIGTERM, windows_shutdown_handler)
    signal.signal(signal.SIGINT, windows_shutdown_handler)

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
