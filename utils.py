import ast
import asyncio
from datetime import datetime
import json
import logging
import os
import re
import psutil
# from upstash_redis.asyncio import Redis
from redis.asyncio import Redis  # Use redis.asyncio for async Redis operations
import yaml
from celery.result import AsyncResult # Import AsyncResult
from enum import Enum
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional
import crawl4ai as _c4


logger = logging.getLogger(__name__)

class TaskStatus(str, Enum):
    READY = "Ready"
    STARTED = "Started"
    SCHEDULED = "Scheduled"
    IN_PROGRESS = "In Progress"
    CANCELED = "Canceled"
    COMPLETED = "Completed"
    FAILED = "Failed"

class FilterType(str, Enum):
    RAW = "raw"
    FIT = "fit"
    BM25 = "bm25"
    LLM = "llm"

def load_config() -> Dict:
    """Load and return application configuration with env var expansion."""
    config_path = Path(__file__).parent / "config.yml"
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

def setup_logging(config: Dict) -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=config["logging"]["level"],
        format=config["logging"]["format"]
    )
async def remove_stale_clients(socket_client) -> None:
    """Remove stale WebSocket clients."""
    
    disconnected_clients = set()
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
    """Handle datetime serialization for JSON."""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
def should_cleanup_task(created_at: str, ttl_seconds: int = 3600) -> bool:
    """Check if task should be cleaned up based on creation time."""
    created = datetime.fromisoformat(created_at)
    return (datetime.now() - created).total_seconds() > ttl_seconds


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

def create_task_response(celery_task: AsyncResult, task: Dict[str, str], task_id: str, base_url: str) -> dict:
    """Create response for task status check."""
    response = {
        "task_id": task_id,
        "status": celery_task.status or task["status"],
        "created_at": task["created_at"],
        "url": task["url"],
        "_links": {
            "self": {"href": f"{base_url}llm/{task_id}"},
            "refresh": {"href": f"{base_url}llm/{task_id}"}
        }
    }

    if task["status"] == TaskStatus.COMPLETED:
        response["result"] = celery_task.result if celery_task.ready() else None or json.loads(task["result"])
    elif task["status"] == TaskStatus.FAILED:
        response["error"] = task["error"]

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




async def stream_pubsub_results(redis: Redis, channel: str, results_gen):
    """Publish results with heartbeats and completion markers to a Redis channel."""
    try:
        data: list[dict[str, Any]] | str = []
        result: _c4.CrawlResult
        complete = {"status": "ok", "message": "completed"}
        async for result in results_gen :
            try:
                server_memory_mb = _get_memory_mb()
                result_dict = result.model_dump()
                result_dict['server_memory_mb'] = server_memory_mb
                result_dict['status'] = "model_dump"
                url = result_dict.get('url', 'unknown')
                logger.info(f"Publishing result for {url}")
                
                # Serialize and return the results, using model_dump if available
                model_dump = result_dict if hasattr(result, 'model_dump') else {"status": "No model_dump available"}
                
                # Append only if dump is a dictionary
                if isinstance(model_dump, dict):
                    data.append(model_dump)
                else:
                    raise ValueError(model_dump)
                
                # This block of code is part of a function that streams results and publishes them to
                # a Redis channel with some additional processing
                await redis.xadd(channel, {"status": "ok", "message": "processing", "url": url, "dump": json.dumps(model_dump, default=datetime_handler) })  # Publish to Redis channel
                
                # give some time to redis
                await asyncio.sleep(1)
            
            except Exception as e:
                logger.error(f"Serialization error: {e}")
                error_response = {"status": "error", "message": str(e), "url": getattr(result, 'url', 'unknown')}
                
                await redis.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in error_response.items() } )  # Publish error response
                complete = {"status": "error", "message": "completed"}
        
        await redis.xadd(channel, {key: str(value) if isinstance(value, bool) else value  for key, value in complete.items()})  # Publish completion marker

        return data
        

    except asyncio.CancelledError:
        logger.warning("Client disconnected during streaming")
    
    # finally:
    #     await redis.close()  # Make sure to properly close the Redis connection