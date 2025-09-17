# ------------- dependency placeholders -------------
import asyncio
from asyncio.log import logger
import json
import logging
import time
from typing import Any, Callable, Dict, Optional, Union

from celery import uuid
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, HttpUrl
from fastapi import APIRouter, Depends, HTTPException, Request, Response, WebSocket, WebSocketDisconnect, status

from redisCache import REDIS_CHANNEL, redis, pure_redis
from api import cancel_a_job, handle_crawl_job, handle_crawl_stream_job, handle_llm_request, handle_markdown_request, handle_stream_task_status, handle_task_status
from auth import get_token_dependency
from crawl import reader
from firestore import FirebaseClient
from schemas import CrawlOperation, CrawlRequest, MarkdownRequest, RawCode

from triggers import event_stream
from utils import load_config, safe_eval_config, setup_logging, stream_results
import gzip

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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized decoded_token")
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
):
    await websocket.accept()
    _socket_client.add(websocket)
    try:
        # while True:
        #     await asyncio.sleep(1)  # Example: periodic event
            try:
                # async for message in pubsub.listen():
                #     if message["type"] == "message":
                #         await websocket.send_text(message["data"])
                await websocket.send_text("Hello, this is a server event!")
                # await asyncio.sleep(10)
                # await websocket.send_json({"event": "heartbeat", "timestamp": time.time()})
            except Exception as e:
                logger.warning(f"WebSocket send failed: {e}")
                # break
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

# ---------- Temporary job ---------------------------------------------------------
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

# ---------- Crawl jobs ---------------------------------------------------------
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
        body.urls, body.f, body.q, body.c if body.c is not None else "0", config, body.browser_config or None
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
# create a temporary task id
@job_router.get("/crawl/job/temp-task-id")
async def create_temp_task_id(
    request: Request,
    decoded_token: Dict = Depends(verify_token)
    ):

    try:
        temp_task_id = str(uuid())  # Generate a new temporary ID
        logger.info(f"temp_task_id: {temp_task_id}")

        await redis.hset(key=f"temp_task_id:{temp_task_id}", field="celery_task_id", value="empty")

        response = {"temp_task_id": temp_task_id}

        return JSONResponse(response)

    except Exception as e:
        logger.error(f"Error creating temporary Task ID: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "error": "Cannot create temporary Task id",
                "internal_message": str(e)
            }
        )

# get the celery task id from temporary task id
@job_router.get("/crawl/job/{temp_task_id}")
async def get_task_id(
    request: Request,
    temp_task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    retries = 0
    while retries < 3:
        try:
            task_id = await redis.hget(f"temp_task_id:{temp_task_id}", "celery_task_id")

            if task_id and task_id != "empty":
                response = {"task_id": task_id}
                return JSONResponse(response)

            retries += 1
            logger.info(f"Retrying to fetch task_id for temp_task_id: {temp_task_id}. Attempt {retries}/3")
            await asyncio.sleep(1)  # Wait before retrying

        except Exception as e:
            logger.error(f"Error fetching task_id for temp_task_id: {temp_task_id}. Error: {str(e)}")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": "error",
                    "error": "Internal server error",
                    "internal_message": str(e)
                }
            )

    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "status": "error",
            "error": "Task not found after multiple attempts"
        }
    )
    

# Cancel and Status general API ENDPOINTS
@job_router.put("/crawl/job/cancel/{temp_task_id}")
async def crawl_job_cancel(
    request: Request,
    temp_task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    """Cancel a running crawl job."""

    if not temp_task_id:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "status": "error",
                "error":  "temporary task id required"
            }
        )
    try: 

        uid = decoded_token.get("uid") or "jwt_disabled"  # This is the user's UID
        return await cancel_a_job(redis, uid, temp_task_id)
    
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            content={
                "status": "error",
                "error": "Cannot cancel the job",
                "internal_message": str(e)
                 }
        )

    

# get status of a crawl job using celery task id
@job_router.get("/crawl/job/status/{task_id}")
async def crawl_job_status(
    request: Request,
    task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    return await handle_task_status(redis, task_id, base_url=str(request.base_url))

# FIXME: ── SSE (Server-Sent Events) stream ──────────────────────────────────────────────────────────────
# get status of a crawl stream job using temporary task id
@job_router.get("/crawl/stream/job/status/{temp_task_id}")
async def crawl_stream_job_status(
    request: Request,
    temp_task_id: str,
    decoded_token: Dict = Depends(verify_token)
):
    retries = 0
    while retries < 3:
        try:
            # Get the task from redis
            task_id = await redis.hget(key=f"temp_task_id:{temp_task_id}", field='celery_task_id')
            
            logger.info(task_id)    
            task = await redis.hgetall(f"task:{task_id}")

            if not task_id or task_id == 'empty' or not task:
                retries += 1
                logger.info(f"Retrying to fetch task_id for temp_task_id: {temp_task_id}. Attempt {retries}/3")
                await asyncio.sleep(1)  # Wait before retrying
                continue

            if not task:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={
                        "status": "error",
                        "error": "Task not found"
                    }
                )
            
            return await handle_stream_task_status(task, task_id, base_url=str(request.base_url))

        except Exception as e:
            logger.error(f"Error fetching task status for temp_task_id: {temp_task_id}. Error: {str(e)}")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "status": "error",
                    "error": "Internal server error",
                    "internal_message": str(e)
                }
            )

    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "status": "error",
            "error": "Task id not found after multiple attempts"
        }
    )

# stream crawl results using celery task id
@job_router.get("/crawl/stream/job/{task_id}")
async def stream_crawl_results(
    task_id: str,
    decoded_token: bool = Depends(verify_token),
    ):
   
    channel = f"{REDIS_CHANNEL}:{task_id}"  # Unique channel for the task
   
    async def event_stream(channel: str):
        completed_yielded = False  # Flag to indicate if the completion message has been yielded
        seen_messages = set()
        retries = 0  # Initialize retry counter
        while True:
            
            await asyncio.sleep(1)  # Simulate waiting for new messages
            try:                
                
                messages = await pure_redis.xread({channel: '0'}, count=None, block=5000)

                if retries > 12 and not completed_yielded:
                    logger.info("No completed message received after multiple retries. Ending stream.")
                    break

                logger.info(f"Received messages from Redis: {len(messages)} messages")

                if messages and isinstance(messages, list):
                    for message in messages:
                        if len(message) < 2:
                            logger.warning(f"Unexpected message format: {message}")
                            continue

                        _, message_list = message

                        for msg_id, msg_data in message_list:
                            if isinstance(msg_data, bytes):
                                msg_data_dict = json.loads(msg_data.decode("utf-8"))
                            elif isinstance(msg_data, dict):
                                msg_data_dict = msg_data
                            else:
                                logger.warning(f"Unexpected msg_data format: {msg_data}")
                                continue
                        
                            if isinstance(msg_data_dict, dict) and "message" in msg_data_dict and msg_data_dict["message"] == "completed":
                                if not completed_yielded:
                                    logger.info("Yielding completed message.")
                                    # ADD 'data: ' PREFIX HERE
                                    yield ("data: " + json.dumps(msg_data_dict, ensure_ascii=False) + "\n").encode('utf-8')
                                    completed_yielded = True
                                    break
                                continue

                            unique_id = f"{msg_data_dict.get('chunk_index', '')}_{msg_data_dict.get('url', msg_id)}"  \
                                if "chunk_index" in msg_data_dict else msg_data_dict.get("id",msg_data_dict.get("url", msg_id))
                            
                            if unique_id in seen_messages:
                                logger.info(f"Duplicate message ignored {msg_id}: {unique_id}")
                                continue

                            seen_messages.add(unique_id)

                            logger.info(f"Received message on str {msg_id}")
                            
                            retries = 0
                            # ADD 'data: ' PREFIX HERE
                            yield  ("data: " + json.dumps(msg_data_dict, ensure_ascii=False) + "\n").encode('utf-8')
                else:
                    logger.warning("No messages returned or malformed response.")
                    
            except Exception as e:
                logger.error(f"Error in event stream: {e}")
            
            finally:
                logger.info("Closing the event stream.")

            if completed_yielded :
                yield b"data: [DONE]\n"
                break
            else:
                retries += 1
                logger.info(f"No completed message received. Continuing to listen for new messages. Retry count: {retries}")

        seen_messages.clear()
    return StreamingResponse(
        event_stream(channel),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Stream-Status": "active",
        })


# Post Crawl stream job enqueue endpoint
@job_router.post("/crawl/stream/job", status_code=202)
async def crawl_stream_job_enqueue(
    request: Request,
    payload: CrawlRequest,
    decoded_token: Dict = Depends(verify_token),
):
    if not payload.urls:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "At least one URL required")
    if not payload.temp_task_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "temp task id missing, is required")
    
    if not payload.operation_data:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "operation data missing, is required")
    if not payload.browser_config:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "browser configuration is missing, is required")
    if not payload.crawler_config:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "crawler configuration data missing, is required")
    
    try:
        uid = decoded_token.get("uid") or "jwt_disabled"  # This is the user's UID
        
        # create new firebase client

        urls = [str(u) for u in payload.urls]
        temp_task_id = payload.temp_task_id

        operation_data = payload.operation_data

    

        return await handle_crawl_stream_job(
                temp_task_id,
                redis,
                uid=uid,
                base_url=str(request.base_url),
                urls=urls,
                operation_data = operation_data,
                browser_config = payload.browser_config,
                crawler_config = payload.crawler_config,
                config=config or {}
            )
    except Exception as e:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e))


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


# @limiter.limit(config["rate_limiting"]["default_limit"])
# @job_router.post("/crawl/stream")
# async def crawl_stream(
#     request: Request,
#     crawl_request: CrawlRequest,
#     decoded_token: bool = Depends(verify_token),
# ):
#     if not crawl_request.urls:
#         raise HTTPException(400, "At least one URL required")
#     crawler, gen = await handle_stream_crawl_request(
#         urls=crawl_request.urls,
#         browser_config=crawl_request.browser_config,
#         crawler_config=crawl_request.crawler_config,
#         config=config,
#     )
#     return StreamingResponse(
#         stream_results(crawler, gen),
#         media_type="application/x-ndjson",
#         headers={
#             "Cache-Control": "no-cache",
#             "Connection": "keep-alive",
#             "X-Stream-Status": "active",
#         },
#     )


