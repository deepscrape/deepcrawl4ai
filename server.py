import asyncio

from contextlib import asynccontextmanager
from functools import wraps
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable

# from typing import Annotated  # noqa: F401
from crawl4ai import AsyncWebCrawler, BrowserConfig
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
)
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
# prometheus fast api
from prometheus_fastapi_instrumentator import Instrumentator

from upstash_ratelimit.asyncio import Ratelimit


from utils import periodic_client_cleanup
from redisCache import default_limiter, test_connection, redis
from crawler_pool import close_all, get_crawler, janitor
import uvicorn

# from crawl import on_browser_created
# from actions import infinite_scroll, load_more  # noqa: F401
from auth import get_token_dependency

# Use uvloop for enhanced performance
from utils import load_config, setup_logging
from job import init_job_router, job_router

# ── internal imports (after sys.path append) ─────────────────
# sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

# ────────────────── configuration / logging ──────────────────
config = load_config()
setup_logging(config)
logger = logging.getLogger(__name__)


__version__ = config["app"]["version"] or "0.5.1-d1"

# ── global page semaphore (hard cap) ─────────────────────────
MAX_PAGES = config["crawler"]["pool"].get("max_pages", 30)
GLOBAL_SEM = asyncio.Semaphore(MAX_PAGES)


orig_arun = AsyncWebCrawler.arun

async def capped_arun(self, *a, **kw):
    async with GLOBAL_SEM:
        return await orig_arun(self, *a, **kw)
AsyncWebCrawler.arun = capped_arun


# Set the number of workers
NUM_WORKERS = int(os.getenv("NUM_WORKERS", os.cpu_count() or 1))

# Store connected WebSocket clients    
socket_client = set()

if sys.platform != "win32":
    import uvloop  # type: ignore
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    from asyncio import WindowsProactorEventLoopPolicy as EventLoopPolicy
    asyncio.set_event_loop_policy(EventLoopPolicy())
    # logger.warning("uvloop is not supported on Windows, using default(auto) event loop")

# ───────────────────── FastAPI lifespan ──────────────────────
@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await get_crawler(BrowserConfig(
            extra_args=config["crawler"]["browser"].get("extra_args", []),
            **config["crawler"]["browser"].get("kwargs", {}),
        ))           # warm‑up
        await test_connection(redis) # Moved from on_event("startup")
        app.state.janitor = asyncio.create_task(janitor())        # idle GC
        app.state.websocket = asyncio.create_task(periodic_client_cleanup(socket_client))
        yield
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        raise
    finally:
        if hasattr(app.state, "janitor"):
            app.state.janitor.cancel()
        if hasattr(app.state, "websocket"):
            app.state.websocket.cancel()
        await close_all()

# Initialize FastAPI app on_startup=[startup_event], on_shutdown=[shutdown_event]
# ───────────────────── FastAPI instance ──────────────────────
app = FastAPI(
    title=config["app"]["title"],
    version=config["app"]["version"],
    lifespan=lifespan,
)
# ───────────────────── FastAPI Rate Limiting ──────────────────────
# # Initialize rate limiter
# limiter = Limiter(
#     key_func=get_remote_address,
#     default_limits=[config["rate_limiting"]["default_limit"]],
#     storage_uri=config["rate_limiting"]["storage_uri"],
#     enabled=config["rate_limiting"].get("enabled", True)
# )


# Define the rate limiting decorator
def rate_limited(
    rate: int = 1,
    limiter: Ratelimit = default_limiter
) -> Callable:
    """Rate limiting decorator for FastAPI endpoints.
    
    Args:
        limit: Rate limit string (e.g. "100/minute", "1000/hour")
        limiter: Rate limiter instance to use (defaults to default_limiter)
    
    Returns:
        Decorator function that applies rate limiting
    """
    def decorator(func: Callable) -> Callable:

        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # Extract Request object
            request = next(
                (arg for arg in args if isinstance(arg, Request)),
                kwargs.get('request')
            )
            
            if not request:
                raise ValueError("Request parameter not found in function arguments")

            # Create unique identifier for this request
            client_ip = request.client.host if request.client else "unknown"
            identifier = f"{client_ip}:{request.url.path}"
            
            print(f"Rate limit identifier: {identifier}")
            # Apply rate limiting
            response = await limiter.limit(identifier, rate)
            
            # Add rate limit headers to response
            request.state.ratelimit = {
                "limit": response.limit,
                "remaining": response.remaining,
                "reset": response.reset
            }

            if not response.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers={
                        "Retry-After": str(response.reset),
                        "X-RateLimit-Limit": str(response.limit),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(response.reset)
                    }
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator
# ───────────────────── FastAPI Security ───────────────────────

def _setup_security(app_: FastAPI):
    sec = config["security"]
    if not sec["enabled"]:
        return
    if sec.get("https_redirect"):
        app_.add_middleware(HTTPSRedirectMiddleware)
    if sec.get("trusted_hosts", []) != ["*"]:
        app_.add_middleware(
            TrustedHostMiddleware, allowed_hosts=sec["trusted_hosts"]
        )

_setup_security(app)

# setup Prometheus metrics and health check endpoints
if config["observability"]["prometheus"]["enabled"]:
    Instrumentator().instrument(app).expose(app)


# Set the token dependency for token verification, jwt if enabled from config file
verify_token = get_token_dependency(config)

# ───────────────────── FastAPI middlewares ──────────────────────

# security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    resp = await call_next(request)
    if config["security"]["enabled"]:
        resp.headers.update(config["security"]["headers"])
    return resp

# Middleware to apply default rate limiting
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Define routes to exclude from rate limiting
    excluded_paths = ["/health", "/status"]

    if request.url.path in excluded_paths:
        return await call_next(request)

    # Use client IP as the identifier
    client_ip = request.client.host if request.client else "unknown"
    identifier = f"{client_ip}:{request.url.path}"
    
    # Apply default rate limiting
    response = await default_limiter.limit(identifier)
    response.remaining
    logger.info(f"Rate limiting for {identifier}, Remaining: {response.remaining}" )
    # Add rate limit headers to response
    request.state.ratelimit = {
        "limit": response.limit,
        "remaining": response.remaining,
        "reset": response.reset
    }

    if not response.allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={
                "Retry-After": str(response.reset),
                "X-RateLimit-Limit": str(response.limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(response.reset)
            }
        )
    else:
        response = await call_next(request)
            
        # Add rate limit headers to the response
        response.headers["X-RateLimit-Limit"] = str(request.state.ratelimit["limit"])
        response.headers["X-RateLimit-Remaining"] = str(request.state.ratelimit["remaining"])
        response.headers["X-RateLimit-Reset"] = str(request.state.ratelimit["reset"])
        return response

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config["app"].get("cors_origins", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# # Add Gzip compression
app.add_middleware(GZipMiddleware, minimum_size=config["app"].get("minimum_size", 1000))

# ─────────────────── Redis connection Global  ─────────────────────
# redis = aioredis.from_url(config["redis"].get("uri", "redis://localhost"))


# ───────────────────── FastAPI routes ───────────────────────

# ── job router ────────────────────────────────────────────── init_job_router(redis, config, verify_token, socket_client)
app.include_router(router=init_job_router(config, socket_client))

# startup router
@app.get("/")
async def root(decoded_token: bool = Depends(verify_token)):
    print(decoded_token)
    return {
        "message": "WebSocket server is running. Connect to /ws/events or /events for stream-events"
    }

# health check endpoint
@app.get(config["observability"]["health_check"]["endpoint"])
async def health():
    return {"status": "ok", "timestamp": time.time(), "version": __version__}

# prometheus metrics endpoint
@app.get(config["observability"]["prometheus"]["endpoint"])
async def metrics():
    return RedirectResponse(config["observability"]["prometheus"]["endpoint"])


# ────────────────────────── cli ──────────────────────────────
if __name__ == "__main__":
    import uvicorn
    # if sys.platform == "win32":
    #     import winloop
    #     winloop.install()
    #     loop = asyncio.get_event_loop()
        # logger.info("Using winloop event loop", loop.run_forever())

    # Winloop's eventlooppolicy will be passed to uvicorn after this point...
    uvicorn.run(
        "server:app",
        host=config["app"]["host"],
        port=config["app"]["port"],
        reload=config["app"]["reload"],
        loop=config["app"]["uvloop"] if sys.platform == "win32" else "uvloop", # force uvloop on unix
        timeout_keep_alive=config["app"]["timeout_keep_alive"],
        workers=int(config["app"]["workers"] or NUM_WORKERS),
    )
# ─────────────────────────────────────────────────────────────
