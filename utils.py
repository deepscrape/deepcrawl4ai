import ast
import asyncio
from datetime import datetime
import json
import logging
import os
import re
from fastapi import WebSocket
import psutil
# from upstash_redis.asyncio import Redis
from redis.asyncio import Redis  # Use redis.asyncio for async Redis operations 
import yaml
from celery.result import AsyncResult # Moved to function scope to avoid circular dependency
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional
import crawl4ai as _c4
import urllib.parse
import random

from redisCache import redis_xadd

logger = logging.getLogger(__name__)

class TaskStatus(str, Enum):
    READY = "Ready"
    STARTED = "Started"
    SCHEDULED = "Scheduled"
    IN_PROGRESS = "In Progress"
    PENDING = "Pending"
    CANCELED = "Canceled"
    REVOKED = "Revoked"
    RETRY = "Retry"
    COMPLETED = "Completed"
    FAILED = "Failed"

class CeleryTaskStatus(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"

class FilterType(str, Enum):
    RAW = "raw"
    FIT = "fit"
    BM25 = "bm25"
    LLM = "llm"

def load_config() -> Dict:
    """
    Load and return application configuration with environment variable expansion.

    This function reads the 'config.yml' file located in the same directory as this script,
    expands any environment variable placeholders in the form of ${VAR} by tagging them with
    '!env_var', and parses the YAML content into a Python dictionary.

    Returns:
        Dict: The parsed configuration as a dictionary. Returns an empty dictionary if
        the file cannot be read or parsed.

    Raises:
        None: All exceptions are caught and result in an empty dictionary being returned.
    """
    config_path = Path(__file__).parent / "config.yml"
    try:
        with open(config_path, "r", encoding='utf-8') as config_file:
            content = config_file.read()
        # Tag all lines containing ${VAR} with !env_var
        content = re.sub(r'^(.*\$\{[^}^{]+\}.*)$', r'!env_var \1', content, flags=re.MULTILINE)
        
        def env_var_constructor(loader, node):
            value = loader.construct_scalar(node)
            return re.sub(r'\$\{([^}^{]+)\}', lambda m: os.environ.get(m.group(1), ""), value)

        class EnvVarLoader(yaml.SafeLoader):
            pass
        EnvVarLoader.add_implicit_resolver('!env_var', re.compile(r'.*\$\{[^}^{]+\}.*'), None)
        EnvVarLoader.add_constructor('!env_var', env_var_constructor)
        return yaml.load(content, Loader=EnvVarLoader)

    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

def setup_logging(config: Dict) -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=config["logging"]["level"],
        format=config["logging"]["format"]
    )
async def remove_stale_clients(socket_client: set[WebSocket]) -> None:
    """Remove stale WebSocket clients."""
    
    disconnected_clients:set[WebSocket] = set()
    for client in socket_client:
        try:
            await client.send_text("ping")  # Ping the client
        except Exception:
            disconnected_clients.add(client)
    for client in disconnected_clients:
        socket_client.remove(client)

async def periodic_client_cleanup(socket_client) -> None:
    """Periodically check and remove stale clients."""
    while True:
        logging.info("Checking for stale clients...")
        await asyncio.sleep(60)  # Check every minute
        await remove_stale_clients(socket_client)

def is_task_id(value: str) -> bool:
    """Check if the value matches task ID pattern."""
    return value.startswith("llm_") and "_" in value

def safe_eval_config(expr: str) -> dict:
    """
    Accept exactly one top‑level call to CrawlerRunConfig(...) or BrowserConfig(...).
    Whatever is inside the parentheses is fine *except* further function calls
    (so no  __import__('os') stuff).  All public names from crawl4ai are available
    when we eval.
    """
    tree = ast.parse(expr, mode="eval")

    # must be a single call
    if not isinstance(tree.body, ast.Call):
        raise ValueError("Expression must be a single constructor call")

    call = tree.body
    if not (isinstance(call.func, ast.Name) and call.func.id in {"CrawlerRunConfig", "BrowserConfig"}):
        raise ValueError(
            "Only CrawlerRunConfig(...) or BrowserConfig(...) are allowed")

    # forbid nested calls to keep the surface tiny
    for node in ast.walk(call):
        if isinstance(node, ast.Call) and node is not call:
            raise ValueError("Nested function calls are not permitted")

    # expose everything that crawl4ai exports, nothing else
    safe_env = {name: getattr(_c4, name)
                for name in dir(_c4) if not name.startswith("_")}
    obj = eval(compile(tree, "<config>", "eval"),
               {"__builtins__": {}}, safe_env)
    return obj.dump()

def datetime_handler(obj: any) -> Optional[str]: # type: ignore
    """Handle datetime serialization for JSON, with a fallback for other types."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    # Fallback for other non-JSON-serializable types, converting them to string
    try:
        return str(obj)
    except Exception:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
def should_cleanup_task(created_at: str, ttl_seconds: int = 3600) -> bool:
    """Check if task should be cleaned up based on creation time."""
    created = datetime.fromisoformat(created_at)
    return (datetime.now() - created).total_seconds() > ttl_seconds

def is_scheduled_time_in_past(scheduled_at: int) -> tuple[bool, datetime]:
    """
    Checks if the given scheduled time (in milliseconds since the epoch) is in the past relative to the current system time.
    Args:
        scheduled_at (int): The scheduled time in milliseconds since the Unix epoch.
    Returns:
        bool: True if the scheduled time is in the past or equal to the current time, False otherwise.
    """
    
    scheduled_at_dt = datetime.fromtimestamp(scheduled_at / 1000)
    now = datetime.now()
    
    return now >= scheduled_at_dt, scheduled_at_dt


def url_to_unique_name(url):
    parsed_url = urllib.parse.urlparse(url)
    slug = re.sub(r"[^a-zA-Z0-9_\-]", "-", parsed_url.path)
    slug = slug.strip("-")
    return f"{parsed_url.netloc}_{slug}"


def task_status_color(status: TaskStatus) -> str:
    """
    Returns the color associated with the given TaskStatus enum member.

    Args:
        status (TaskStatus): The task status enum member.

    Returns:
        str: The color associated with the status.
    """
    color_map = {
        TaskStatus.IN_PROGRESS: 'cyan',
        TaskStatus.FAILED: 'red',
        TaskStatus.CANCELED: 'deep-orange',
        TaskStatus.SCHEDULED: 'emerald',
        TaskStatus.COMPLETED: 'green',
        TaskStatus.STARTED: 'blue',
        TaskStatus.READY: 'gray',
        TaskStatus.PENDING: 'deep-purple',
        TaskStatus.RETRY: 'yellow',
    }
    return color_map.get(status, 'gray')

def decode_redis_hash(hash_data: Dict[bytes, bytes] | Dict[str, str]) -> Dict[str, str]:
    """Decode Redis hash data from bytes to strings."""
    result = {}
    for k, v in hash_data.items():
        if isinstance(k, bytes):
            k = k.decode('utf-8')
        if isinstance(v, bytes):
            v = v.decode('utf-8')
        result[k] = v
    return result

# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None

def convert_celery_status(celery_status: CeleryTaskStatus) -> TaskStatus:
    status_mapping = {
        CeleryTaskStatus.PENDING: TaskStatus.PENDING,
        CeleryTaskStatus.STARTED: TaskStatus.IN_PROGRESS,
        CeleryTaskStatus.SUCCESS: TaskStatus.COMPLETED,
        CeleryTaskStatus.FAILURE: TaskStatus.FAILED,
        CeleryTaskStatus.RETRY: TaskStatus.RETRY,
        CeleryTaskStatus.REVOKED: TaskStatus.CANCELED
    }
    
    return status_mapping.get(celery_status, TaskStatus.READY)  # Default to READY if status is unknown

def create_task_status_response(celery_task: AsyncResult, task: Dict[str, str], task_id: str, base_url: str) -> dict:
    """Create response for task status check."""
    response = {
        "task_id": task_id,
        "status": convert_celery_status(celery_task.state) or task["status"],
        "created_at": task["created_at"],
        "urls": task.get("urls", ""),
        "_links": {
            "self": {"href": f"{base_url}llm/{task_id}"},
            "refresh": {"href": f"{base_url}llm/{task_id}"}
        }
    }

    if task["status"] == TaskStatus.COMPLETED or celery_task.ready():
        # Handle successful tasks
        if celery_task.successful():
            # Always prioritize Celery result, even if it's None or empty
            response["result"] = celery_task.result
        # Only fall back to Redis result if Celery result is not available
        elif not hasattr(celery_task, 'result') and task.get("result"):
            try:
                # Try to parse Redis task result as JSON
                response["result"] = json.loads(task["result"])
            except json.JSONDecodeError:
                # If parsing fails, use it as a string
                response["result"] = task["result"]
        else:
            # Set explicit None if no result is available
            response["result"] = None
    elif task["status"] == TaskStatus.FAILED or celery_task.failed():
        response["error"] = task.get("error", "Unknown error")
        response["result"] = celery_task.result

    return response

async def stream_results(crawler: _c4.AsyncWebCrawler, results_gen: AsyncGenerator) -> AsyncGenerator[bytes, None]:
    """Stream results with heartbeats and completion markers."""
    import json
    from utils import datetime_handler

    try:
        async for result in results_gen:
            try:
                server_memory_mb = _get_memory_mb()
                result_dict = result.model_dump()
                result_dict['server_memory_mb'] = server_memory_mb
                logger.info(f"Streaming result for {result_dict.get('url', 'unknown')}")
                data = json.dumps(result_dict, default=datetime_handler) + "\n"
                yield data.encode('utf-8')
            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"error": str(e), "url": getattr(result, 'url', 'unknown')}
                yield (json.dumps(error_response) + "\n").encode('utf-8')

        yield json.dumps({"status": "completed"}).encode('utf-8')
        
    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
    finally:
        try:
            await crawler.close()
        except Exception as e:
            logger.error(f"Crawler cleanup error: {e}")
        pass




async def stream_pubsub_results(redis: Redis, channel: str, results_gen: AsyncGenerator, chunk_size: int = 2048) -> bool:
    """
    Publish results to a Redis stream using pipeline batching.
    Each result is published as a stream entry in batches.
    Error entries are published if serialization fails.
    A final 'completed' marker is always published.
    Returns a list of successfully published result dicts.
    """
    
    result: _c4.CrawlResult
    complete = {"status": "ok", "message": "completed"}
    # buffer: list[dict[str, Any]] = []
    pipe2 = redis.pipeline()
    try:
        async for result in results_gen:
            try:
                server_memory_mb = _get_memory_mb()
                result.html = ""  # Clear HTML content to reduce size
                
                result_dict = result.model_dump()
                # remove html from result before sending to redis

                result_dict["html"] = ""  # type: ignore
                result_dict['server_memory_mb'] = server_memory_mb
                # result_dict['status'] = "model_dump"
                url = result_dict.get('url', 'unknown')

                model_dump = result_dict if hasattr(result, 'model_dump') \
                    else {"status": "error", "message": "No model_dump available skipping", 
                          "url": getattr(result, 'url', 'unknown')}

                if isinstance(model_dump, dict):
                    # data.append(model_dump)
                    logger.info(f"Publishing result for {url}")
                    # buffer.append(model_dump)
                else:
                    raise ValueError(model_dump)

                batch_json = json.dumps(model_dump, default=datetime_handler, ensure_ascii=False)
                pipe = redis.pipeline()
                chunk_size = 4096  # Define chunk_size as a constant (adjust as needed)
                total_chunks = (len(batch_json) + chunk_size - 1) // chunk_size  # Calculate total chunks
                # Split batch_json into chunks of chunk_size
                for i in range(0, len(batch_json), chunk_size):
                    chunk = batch_json[i:i+chunk_size]
                    pipe.xadd(channel, {
                        "status": "ok",
                        "message": "processing",
                        "type": "batch_chunk",
                        "url": url,
                        "chunk_index": str(i // chunk_size),
                        "total_chunks": str(total_chunks),  # Add total_chunks attribute
                        "dump": chunk #.encode("utf-8") if isinstance(chunk, str) else chunk
                    })
                await pipe.execute()

            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"status": "error", "message": str(e), "url": getattr(result, 'url', 'unknown')}
                
                pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in error_response.items() } )
                complete = {"status": "error", "message": "completed"}

        pipe2.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in complete.items()})

    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
        pipe2.xadd(channel, {"status": "canceled", "message": "streaming canceled"})
    except Exception as e:
        logger.error(f"Unexpected error in stream_pubsub_results: {e}")
        pipe2.xadd(channel, {"status": "error", "message": str(e)})
        await pipe2.execute()
        return False

    await pipe2.execute()
    return True


async def retry_async(func, *args, retries=3, base_delay=0.5, max_delay=5, **kwargs):
    """Retry async function with exponential backoff and jitter."""
    attempt = 0
    last_exception = None
    while attempt < retries:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** attempt) + random.uniform(0, 0.5))
            logger.warning(f"Retry {attempt}/{retries} for {func.__name__} due to error: {e}. Waiting {delay:.2f}s.")
            await asyncio.sleep(delay)
    if last_exception:
        raise last_exception
    # This case should ideally not be reached if retries > 0 and func always raises on failure
    raise RuntimeError("Function failed after multiple retries without capturing an exception.")
