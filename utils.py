import ast
import asyncio
from datetime import datetime
import json
import logging
import os
import re
import yaml

from enum import Enum
from pathlib import Path
from typing import Dict
import crawl4ai as _c4


class TaskStatus(str, Enum):
    PROCESSING = "processing"
    FAILED = "failed"
    COMPLETED = "completed"

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

def should_cleanup_task(created_at: str, ttl_seconds: int = 3600) -> bool:
    """Check if task should be cleaned up based on creation time."""
    created = datetime.fromisoformat(created_at)
    return (datetime.now() - created).total_seconds() > ttl_seconds


def decode_redis_hash(hash_data: Dict[bytes, bytes]) -> Dict[str, str]:
    """Decode Redis hash data from bytes to strings."""
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in hash_data.items()}


def create_task_response(task: Dict[str, str], task_id: str, base_url: str) -> dict:
    """Create response for task status check."""
    response = {
        "task_id": task_id,
        "status": task["status"],
        "created_at": task["created_at"],
        "url": task["url"],
        "_links": {
            "self": {"href": f"{base_url}/llm/{task_id}"},
            "refresh": {"href": f"{base_url}/llm/{task_id}"}
        }
    }

    if task["status"] == TaskStatus.COMPLETED:
        response["result"] = json.loads(task["result"])
    elif task["status"] == TaskStatus.FAILED:
        response["error"] = task["error"]

    return response