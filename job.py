# ------------- dependency placeholders -------------
import asyncio
from asyncio.log import logger
import logging
from typing import Any, Callable, Dict, Optional, Union

from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, HttpUrl
from fastapi import APIRouter, Depends, HTTPException, Request, Response, WebSocket, WebSocketDisconnect

from redisCache import redis
from api import cancel_a_job, handle_crawl_job, handle_llm_request, handle_markdown_request, handle_task_status
from auth import get_token_dependency
from crawl import reader
from firestore import FirebaseClient
from schemas import MarkdownRequest, RawCode

from triggers import event_stream
from utils import load_config, safe_eval_config, setup_logging

config = load_config()
setup_logging(config)
logger = logging.getLogger(__name__)

# Type definition for the verify_token callable
VerifyTokenCallable = Union[
    Callable[[HTTPAuthorizationCredentials], bool],
    Callable[[], None]
]


# redis: Redis        # will be injected from server.py
_config: Union[Dict, None] = None
_socket_client: set[Any]

# verify_token: Callable = lambda: None  # dummy until injected

# public router
job_router = APIRouter()


# ---------- payload models --------------------------------------------------
class LlmJobPayload(BaseModel):
    url:    HttpUrl
    q:      str
    schema: Optional[str] = None
    cache:  bool = False


class CrawlJobPayload(BaseModel):
    urls:           list[HttpUrl]
    browser_config: Dict = {}
    crawler_config: Dict = {}


# # === init hook called by server.py =========================================
def init_job_router(config, socket_client: set[Any]) -> APIRouter:
    """Inject shared singletons and return the router for mounting."""
    global _config, _socket_client
    _config, _socket_client = config, socket_client

    return job_router


verify_token = get_token_dependency(config)


# ---------- General endpoints ----------------------------------------------
@job_router.get("/user/data")
async def get_user_data(
    request: Request,
    response: Response,
    decoded_token: Dict = Depends(verify_token)
):
    if not decoded_token:
        raise HTTPException(status_code=401, detail="Unauthorized decoded_token")
    # create new firebase client
    client: FirebaseClient = FirebaseClient()

    # init client firebase
    db, auth = client.init_firebase()
    user_data = {}
    user_id = decoded_token.get("uid")
    logger.info(f"Requesting user data for user: {user_id}")
    user_ref = db.collection("users").document(user_id)
    user_data = user_ref.get().to_dict()
    if user_data:
        return user_data
    else:
        return {"message": "No data found for user"}


@job_router.post("/config/dump")
async def config_dump(raw: RawCode):
    try:
        return JSONResponse(safe_eval_config(raw.code.strip()))
    except Exception as e:
        raise HTTPException(400, str(e))

# ---------- WebSocket endpoint----------------------------------------------
@job_router.websocket("/ws/events")
async def websocket_endpoint(
    websocket: WebSocket, 
    # decoded_token: bool = Depends(verify_token)
):
    await websocket.accept()
    _socket_client.add(websocket)
    try:
        while True:
            await asyncio.sleep(1)  # Example: periodic event
            try:
                await websocket.send_text("Hello, this is a server event!")
            except Exception as e:
                logger.warning(f"WebSocket send failed: {e}")
                break
    except WebSocketDisconnect:
        _socket_client.remove(websocket)
        logger.info("WebSocket: Client disconnected")
    finally:
        _socket_client.discard(websocket)
        logger.info("WebSocket: Client disconnected (cleanup)")

# ---------- LL​M job ---------------------------------------------------------

@job_router.post("/llm/job", status_code=202)
async def llm_job_enqueue(
        payload: LlmJobPayload,
        request: Request,
        decoded_token: Dict = Depends(verify_token),   # late-bound dep
):
    return await handle_llm_request(
        redis,
        request,
        str(payload.url),
        query=payload.q,
        schema=payload.schema,
        cache="1" if payload.cache else "0",
        config=config,
    )

@job_router.get("/llm/job/{task_id}")
async def llm_job_status(
    request: Request,
    task_id: str,
    decoded_token: bool = Depends(verify_token)
):
    return await handle_task_status(redis, task_id)


# FIXME: this is a temporary endpoint for testing
@job_router.post("/crawl")
async def crawl(
    request: Request, 
    response: Response, 
    decoded_token: bool = Depends(verify_token)
):
    """Reader endpoint."""
    try:
        return await reader(request, response)
    except HTTPException as e:
        # Handle specific HTTP exceptions here if needed
        return {"error": str(e.detail)}
    except Exception as e:
        # Handle other exceptions
        logger.warning(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@job_router.post("/crawl/md")
async def get_markdown(
    request: Request,
    body: MarkdownRequest,
    decoded_token: bool = Depends(verify_token),
):
    logger.info(f"Received request: {request.method} {request.url}", )
    
    if not body.urls:
            raise HTTPException(400, "At least one URL required")

    for url in body.urls:
        if not url.startswith(("http://", "https://")):
            raise HTTPException(
                400, "URL must be absolute and start with http/https")

    markdowns, server_processing_time_s, server_memory_delta_mb, server_peak_memory_mb = await handle_markdown_request(
        body.urls, body.f, body.q, body.c if body.c is not None else "0", config
    )
    return JSONResponse({
        "results": markdowns,
        "filter": body.f,
        "query": body.q,
        "cache": body.c,
        "server_processing_time_s": server_processing_time_s,
        "server_memory_delta_mb": server_memory_delta_mb,
        "server_peak_memory_mb": server_peak_memory_mb,
        "success": True
    })


@job_router.post("/crawl/job", status_code=202)
async def crawl_job_enqueue(
        request: Request,
        payload: CrawlJobPayload,
        decoded_token: bool = Depends(verify_token)
):
    return await handle_crawl_job(
        redis,
        [str(u) for u in payload.urls],
        payload.browser_config,
        payload.crawler_config,
        config=config or {},
    )
@job_router.post("/crawl/job/cancel/{task_id}")
async def crawl_job_cancel(
    request: Request,
    task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    """Cancel a running crawl job."""
    return await cancel_a_job(redis, task_id)

@job_router.get("/crawl/job/{task_id}")
async def crawl_job_status(
    request: Request,
    task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    return await handle_task_status(redis, task_id, base_url=str(request.base_url))

# FIXME: ── SSE (Server-Sent Events) stream ──────────────────────────────────────────────────────────────
@job_router.post("/crawl/stream")
async def stream(request: Request,
                 decoded_token: bool = Depends(verify_token)
):
    """Event stream endpoint."""
    try:
        data = await request.json()
        url = data.get("url")
        return StreamingResponse(event_stream(url), media_type="text/event-stream")
    except HTTPException as e:
        # Handle specific HTTP exceptions here if needed
        return {"error": str(e.detail)}
    except Exception as e:
        # Handle other exceptions
        logger.warning(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

""" @app.post("/crawl/stream")
@limiter.limit(config["rate_limiting"]["default_limit"])
async def crawl_stream(
    request: Request,
    crawl_request: CrawlRequest,
    _td: Dict = Depends(token_dep),
):
    if not crawl_request.urls:
        raise HTTPException(400, "At least one URL required")
    crawler, gen = await handle_stream_crawl_request(
        urls=crawl_request.urls,
        browser_config=crawl_request.browser_config,
        crawler_config=crawl_request.crawler_config,
        config=config,
    )
    return StreamingResponse(
        stream_results(crawler, gen),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Stream-Status": "active",
        },
    )
 """
