# from fastapi import FastAPI, BackgroundTasks
# from pydantic import BaseModel
# from typing import List, Dict
# from upstash_redis import Redis
# import json
# import uuid
# import time
# from firebase_admin import firestore, initialize_app
# import firebase_admin


import asyncio
from datetime import datetime, timezone
from functools import partial
import json
import logging
import os
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, cast
from urllib.parse import unquote
from celery.result import AsyncResult # Import AsyncResult here
from celery_app import celery_app # Import celery_app here
from courlan import get_base_url
from crawl4ai import AsyncWebCrawler, BM25ContentFilter, BrowserConfig, CacheMode, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, LLMConfig, LLMContentFilter, LLMExtractionStrategy, LXMLWebScrapingStrategy, MemoryAdaptiveDispatcher, PruningContentFilter, RateLimiter
from crawl4ai.utils import perform_completion_with_backoff
from schemas import CrawlOperation
from fastapi import HTTPException, Request,status
from fastapi.responses import JSONResponse, StreamingResponse
import signal
import time
from datetime import datetime # Import datetime for Celery task ID generation
from celery.result import AsyncResult # Import AsyncResult

from upstash_redis.asyncio import Redis

from crawler_pool import cancel_crawler
from crawlstore import setCrawlOperation, updateCrawlOperation
from firestore import FirebaseClient, db
from monitor import WorkerMonitor
from redisCache import REDIS_CHANNEL
from scrape import should_process_tasks
from utils import FilterType, TaskStatus, _get_memory_mb, convert_celery_status, create_task_status_response, decode_redis_hash, is_task_id, should_cleanup_task, task_status_color
from celery_app import celery_app # Import celery_app
from tasks import crawl_stream_task, crawl_task, llm_extraction_task # Import Celery tasks

logger = logging.getLogger(__name__)
# At module level
_firebase_client = None
_db_instance = None

async def get_firebase_client():
    global _firebase_client, _db_instance
    if _firebase_client is None:
        _firebase_client = FirebaseClient()
        _db_instance, _ = _firebase_client.init_firebase()
    return _db_instance


# class ScrapingOperation(BaseModel):
#     urls: List[str]
#     metadata: Dict = {}


# @app.post("/scrape")
# async def submit_scraping_job(
#     operation: ScrapingOperation, background_tasks: BackgroundTasks
# ):
#     """
#     Submit a new scraping job to the queue
#     """
#     operation_id = str(uuid.uuid4())
#     job = {
#         "id": operation_id,
#         "urls": operation.urls,
#         "metadata": operation.metadata,
#         "timestamp": time.time(),
#     }

#     # Store job metadata in Firestore
#     doc_ref = firestore_client.collection("scraping_jobs").document(operation_id)
#     doc_ref.set(job)

#     # Enqueue the job in Redis
#     await redis.rpush("pending_operations", json.dumps(job))

#     return {
#         "operation_id": operation_id,
#         "status": "queued",
#         "urls_count": len(operation.urls),
#         "firestore_doc_id": doc_ref.id,
#     }


# @app.get("/operation/{operation_id}")
# async def get_operation_status(operation_id: str):
#     """
#     Retrieve the status of a specific operation
#     """
#     # Check Firestore for job metadata
#     job_ref = firestore_client.collection("scraping_jobs").document(operation_id)
#     job_doc = job_ref.get()

#     if not job_doc.exists:
#         return {"status": "not_found"}

#     # Check completed and failed queues in Redis
#     completed = await redis.lrange("completed_operations", 0, -1)
#     failed = await redis.lrange("failed_operations", 0, -1)

#     for op in completed:
#         op_data = json.loads(op)
#         if op_data.get("id") == operation_id:
#             return {
#                 "status": "completed",
#                 "result": op_data,
#                 "job_metadata": job_doc.to_dict(),
#             }

#     for op in failed:
#         op_data = json.loads(op)
#         if op_data.get("operation", {}).get("id") == operation_id:
#             return {
#                 "status": "failed",
#                 "error": op_data,
#                 "job_metadata": job_doc.to_dict(),
#             }

#     return {"status": "pending", "job_metadata": job_doc.to_dict()}


# @app.get("/results/{operation_id}")
# async def get_operation_results(operation_id: str):
#     """
#     Retrieve detailed scraping results from Firestore
#     """
#     # Find results in Firestore
#     results_ref = firestore_client.collection("scrape_results")
#     query = results_ref.where("operation_id", "==", operation_id)
#     results_docs = query.stream()

#     results = [doc.to_dict() for doc in results_docs]

#     return {"operation_id": operation_id, "results": results}


async def handle_llm_request(
    redis: Redis,
    request: Request,
    input_path: str,
    query: Optional[str] = None,
    schema: Optional[str] = None,
    cache: str = "0",
    config: Optional[dict] = None
) -> JSONResponse:
    """Handle LLM extraction requests."""
    base_url = get_base_url(request)
    
    try:
        if is_task_id(input_path):
            return await handle_task_status(
                redis, input_path, base_url
            )

        if not query:
            return JSONResponse({
                "message": "Please provide an instruction",
                "_links": {
                    "example": {
                        "href": f"{base_url}llm/{input_path}?q=Extract+main+content",
                        "title": "Try this example"
                    }
                }
            })

        return await create_new_task(
            redis,
            input_path,
            query,
            schema,
            cache,
            base_url,
            config if config else {}
        )

    except Exception as e:
        logger.error(f"LLM endpoint error: {str(e)}", exc_info=True)
        return JSONResponse({
            "error": str(e),
            "_links": {
                "retry": {"href": str(request.url)}
            }
        }, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)




async def handle_markdown_request(
    urls: List[str],
    filter_type: FilterType,
    query: Optional[str] = None,
    cache: str = "0",
    config: Optional[dict] = None,
    browser_config: Optional[Dict] = None
) -> tuple[list[dict], float | None, Any | None, Any | int | None]:
    """Handle markdown generation requests."""
    start_mem_mb = _get_memory_mb() # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    try:
        urls = [('https://' + unquote(url)) if not url.startswith(('http://', 'https://')) else url for url in urls]
        if filter_type == FilterType.RAW:
            md_generator = DefaultMarkdownGenerator()
        else:
            safe_config = config or {}
            llm_config_dict = safe_config.get("llm", {}) or {}
            content_filter = {
                FilterType.FIT: PruningContentFilter(),
                FilterType.BM25: BM25ContentFilter(user_query=query or ""),
                FilterType.LLM: LLMContentFilter(
                    llm_config=LLMConfig(
                        provider=llm_config_dict.get("provider", ""),
                        api_token=os.environ.get(llm_config_dict.get("api_key_env", ""), ""),
                    ),
                    instruction=query or "Extract main content"
                )
            }[filter_type]
            md_generator = DefaultMarkdownGenerator(content_filter=content_filter)

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY
        crawler_config = CrawlerRunConfig(
                    markdown_generator=md_generator,
                    scraping_strategy=LXMLWebScrapingStrategy(),
                    cache_mode=cache_mode
                )
        
        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=safe_config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(safe_config["crawler"]["rate_limiter"]["base_delay"])
            ) if safe_config["crawler"]["rate_limiter"]["enabled"] else None
        )
        
        if browser_config is not None:
            browser = BrowserConfig.load(browser_config)
        else:
            browser = BrowserConfig(
                headless=True,
                extra_args=[
                "--disable-gpu",  # Disable GPU acceleration
                "--disable-dev-shm-usage",  # Disable /dev/shm usage
                "--no-sandbox",  # Required for Docker
                ],
                viewport={
                    "width": 800,
                    "height": 600,
                },  # Smaller viewport for better performance
            )

        # Initialize the crawler
        from crawler_pool import get_crawler
        crawler, _ = await get_crawler(browser) # Unpack the tuple
        
        results = []
        func = getattr(crawler, "arun" if len(urls) == 1 else "arun_many")
        partial_func = partial(func, 
                                urls[0] if len(urls) == 1 else urls, 
                                config=crawler_config, 
                                dispatcher=dispatcher)
        results = await partial_func()



        end_mem_mb = _get_memory_mb() # <--- Get memory after
        end_time = time.time()
        total_time = end_time - start_time
        
        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb) # <--- Get peak memory
            logger.info(f"Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB, Total Time: {total_time}" )

        # Check each result
        for result in results:
            if not result.success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to process URL {result.url}: {result.error_message}"
                )

        # Return the results
        return (
            [{"url": url, 
              "markdown": result.markdown.raw_markdown if filter_type == FilterType.RAW else result.markdown.fit_markdown,
             } for url, result in zip(urls, results)],           
            total_time,
            mem_delta_mb,
            peak_mem_mb
        )

    except Exception as e:
        logger.error(f"Markdown error: {str(e)}", exc_info=True)
        # Check if crawler was initialized and started, and if it's ready
        if 'crawler' in locals() and isinstance(crawler, AsyncWebCrawler) and crawler.ready:
             try:
                 await crawler.close()
             except Exception as close_e:
                  logger.error(f"Error closing crawler during exception handling: {close_e}")
            
        # Measure memory even on error if possible
        end_mem_mb_error = _get_memory_mb()
        if start_mem_mb is not None and end_mem_mb_error is not None:
            mem_delta_mb = end_mem_mb_error - start_mem_mb

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=json.dumps({ # Send structured error
                "error": str(e),
                "server_memory_delta_mb": mem_delta_mb,
                "server_peak_memory_mb": max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb_error or 0)
            })
        )

async def handle_stream_crawl_request(
    urls: List[str],
    browser_config: dict,
    crawler_config: dict,
    config: dict
) -> Tuple[AsyncWebCrawler, AsyncGenerator]:
    """Handle streaming crawl requests."""
    try:
        browser_conf = BrowserConfig.load(browser_config)
        # browser_config.verbose = True # Set to False or remove for production stress testing
        browser_conf.verbose = False
        crawler_conf = CrawlerRunConfig.load(crawler_config)
        crawler_conf.scraping_strategy = LXMLWebScrapingStrategy()
        crawler_conf.stream = True

        crawler_cfg = (config.get("crawler") or {})
        crl_cfg = (config.get("rate_limiter") or {})

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=crawler_cfg.get("memory_threshold_percent", 80),
             rate_limiter=RateLimiter(
                base_delay=tuple(crl_cfg.get("base_delay", (0.2, 1.0)))
            ) if crl_cfg.get("enabled", False) else None
        )

        from crawler_pool import get_crawler
        bcrawler:tuple[AsyncWebCrawler, str] = await get_crawler(browser_conf)
        crawler, _ = bcrawler
        # crawler = AsyncWebCrawler(config=browser_config)
        # await crawler.start()

        results_gen: AsyncGenerator = cast(
            AsyncGenerator,
            await crawler.arun_many(
            urls=urls,
            config=crawler_conf,
            dispatcher=dispatcher
            )
        )

        return crawler, results_gen

    except Exception as e:
        # Make sure to close crawler if started during an error here
        if 'crawler' in locals() and crawler.ready:
            #  try:
            #       await crawler.close()
            #  except Exception as close_e:
            #       logger.error(f"Error closing crawler during stream setup exception: {close_e}")
            logger.error(f"Error closing crawler during stream setup exception: {str(e)}")
        logger.error(f"Stream crawl error: {str(e)}", exc_info=True)
        # Raising HTTPException here will prevent streaming response
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    
async def handle_crawl_job(
    redis: Redis,
    urls: List[str],
    browser_config: Dict,
    crawler_config: Dict,
    config: Dict,
) -> Dict:
    """
    Fire-and-forget version of handle_crawl_request.
    Creates a task in Redis, runs the heavy work in a background task,
    lets /crawl/job/{task_id} polling fetch the result.
    """
    logger.info(f"Enqueuing crawl job for URLs: {urls}")
    # print crawler_config
    logger.info(f"Crawler config: {crawler_config}")
    # Enqueue the task to Celery
    task = crawl_task.delay(urls, browser_config, crawler_config)
    task_id = task.id

    # Store initial task details in Redis, signature will be updated by worker
    await redis.hset(f"task:{task_id}", values={
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "created_at": datetime.now(timezone.utc).isoformat(),
        "urls": json.dumps(urls),
        "result": "",
        "error": "",
    })
    return {"task_id": task_id}

async def handle_crawl_stream_job(
        temp_task_id: str,
        redis: Redis,
        uid: str,
        base_url: str,
        urls: List[str],
        operation_data: Dict,
        browser_config: Dict,
        crawler_config: Dict,
        config: Dict,
) -> JSONResponse:
    
    """ Fire-and-forget version of handle_stream_crawl_request.
    Creates a task in Redis, runs the heavy work in a background task and publish to redis pub/sub,
    lets /crawl/stream/job polling fetch the result by SSE """

    logger.info(f"Enqueuing a streaming crawl job for URLs: {urls}")
    # print crawler_config
    logger.info(f"Crawler config: {crawler_config}")
    
    # # Only process if system is healthy
    # if await should_process_tasks(monitor.metrics):

    # data: CrawlOperation
    doc_ref = db.collection(f"users/{uid}/operations").document()
    operation_id = doc_ref.id

    # Enqueue the task to Celery
    task = crawl_stream_task.delay(uid, operation_id, urls, browser_config, crawler_config)

    # get the celery task id
    task_id = task.id

    operation_data["task_id"] = task_id  # Add the task ID to operation_data

    # save To firestore
    await setCrawlOperation(uid, operation_data, db, doc_ref)
    
    pipe = redis.pipeline()
    # this method is temporary, and is used for celery task cancelation 
    pipe.hset(f"temp_task_id:{temp_task_id}", "celery_task_id", task_id)

    # Store initial task details in Redis, signature will be updated by worker
    pipe.hset(f"task:{task_id}", values={
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "created_at": datetime.now(timezone.utc).isoformat(),
        "urls": json.dumps(urls),
        "temp_task_id": temp_task_id,
        "operation_id": operation_id,
        "result": "",
        "error": "",
    })

    await pipe.exec()


    # return a json response by status and reference links
    return JSONResponse({
        "task_id": task_id,
        "temp_task_id": temp_task_id,
        "operationId": operation_id,
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "_links": {
            "self": {"href": f"{base_url}crawl/stream/job/{task_id}"},
            "status": {"href": f"{base_url}crawl/stream/job/{task_id}"},
            "cancel": {"href": f"{base_url}crawl/job/cancel/{temp_task_id}"}
        }
    })

    # if res[1] == "OK":
        
    # else:
    #     raise ValueError("Failed to execute Redis pipeline")


async def cancel_a_job(
        redis: Redis,
        uid: str,
        temp_task_id: str, 
        *,
        force: bool = False):

    """Cancel a running crawl job using Celery's revocation."""
    
    response = {"status": "ok", "message": f"Task {temp_task_id} canceled successfully."}

    task_id = await redis.hget(key=f"temp_task_id:{temp_task_id}", field='celery_task_id')
    if isinstance(task_id, bytes):
        task_id = task_id.decode('utf-8')

    task_raw = await redis.hgetall(f"task:{task_id}")

    task_info = decode_redis_hash(task_raw) if task_raw else {}

    operation_id = task_info.get("operation_id")

    if not task_info:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        browser_sign = task_info["signature"] if hasattr(task_info, "no_signature") else None
        if (browser_sign and browser_sign != "no_signature"):
            await cancel_crawler(browser_sign)  # Remove the crawler to free resources


        celery_task = AsyncResult(task_id, app=celery_app)

        if not celery_task.ready():
            # Revoke the Celery task
            # Windows-compatible task revocation
            if os.name == 'nt':  # Windows
                celery_task.revoke(terminate=True)

                # Force termination on Windows needs special handling
                if force:
                    # Send shutdown event to worker
                    celery_app.control.broadcast('shutdown')
            else:  # POSIX systems
                celery_task.revoke(
                    terminate=True,
                    signal=signal.SIGKILL if force else signal.SIGTERM
                )
        deadline = time.monotonic() + 15  # seconds    
        while True:
            # Check the task status to see if it's finished
            if celery_task.ready():
                
                # print at the screen
                logger.info(f"celery status: {celery_task.status}")

                # if these two conditions approved break from loop
                if celery_task.successful() or celery_task.failed():
                    response = {"status": "warning", "message": f"The task {temp_task_id} can’t be canceled because it has already finished."}
                    break

                # if task revoked or canceled
                else:
                    # # init client firebase
                    # db = await get_firebase_client()

                    # convert pydantic class to celery class task
                    status = convert_celery_status(celery_task.status)  # Get the final status of the task
                    
                    # calculate the end time
                    duration = time.time() - datetime.fromisoformat(task_info["created_at"]).timestamp()
                    pipe = redis.pipeline()
                    pipe.hset(
                        f"operation_metrics:{operation_id}",
                        values={
                            "duration": duration,
                            "status": status,
                        },
                    )

                    # update redis
                    pipe.hset(f"task:{task_id}",
                        values={
                            "status": status.value, 
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "result": json.dumps(celery_task.result) if isinstance(celery_task.result, (dict, list)) else str(celery_task.result),
                    })

                    await pipe.exec()

                    # if pipeOK[1] == "OK":
                    #     pipeOK[0]
                    # else:
                    #     logger.error(f"Failed to update task {task_id} status in Redis after cancellation.")

                    if task_id and uid and uid != "jwt_disabled" and operation_id:
                        
                        # TODO: FIREBASE UPDATE - change to redis update
                        # Update the operation status in the fire database
                        await updateCrawlOperation(
                            uid,
                            operation_id,
                            {
                                "status": TaskStatus.CANCELED,
                                "color": task_status_color(TaskStatus.CANCELED),
                                "updated_At": datetime.now(timezone.utc).isoformat(),
                                "duration": duration,
                            },
                            db,
                        )
                    
                    break
            
            if time.monotonic() > deadline:
                logger.warning("Timeout waiting for task %s to cancel", task_id)
                break

            await asyncio.sleep(0.3)  # Wait before checking again

        return response
    
    except Exception as e:
        logger.error(f"Error revoking Celery task {task_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {e}")

async def handle_task_status(
    redis: Redis,
    task_id: str,
    base_url: str = "",
    *,
    keep: bool = False
) -> JSONResponse:
    """Handle task status check requests."""

    # get the task from redis
    task = await redis.hgetall(f"task:{task_id}")

    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "status": "error",
                "message": "Task not found"
            }
        )
    celery_task = AsyncResult(task_id, app=celery_app) # Re-initialize celery_task here

    # query celery task state by task id
    task = decode_redis_hash(task)
    temp_task_id = task.get("temp_task_id")
    response = create_task_status_response(celery_task, task, task_id, base_url)

    # remove task from redis keep metadata in firebase
    status_str = response["status"].value if isinstance(response["status"], TaskStatus) else response["status"]
    if status_str in {TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELED.value}:
        if not keep and should_cleanup_task(task["created_at"]):
            pipe = redis.multi()
            pipe.delete(f"task:{task_id}")
            pipe.delete(f"{REDIS_CHANNEL}:{task_id}")
            pipe.delete(f"celery-task-meta-{task_id}")
            if temp_task_id:
                pipe.delete(f"temp_task_id:{temp_task_id}")
            await pipe.exec()

    return JSONResponse(response)

async def handle_stream_task_status(
    task,
    task_id,
    base_url: str = "",
):
    """Stream status updates for a task."""
    try:
        task = decode_redis_hash(task)
        
        async def stream_task_status():
            try:
                while True:
                    # Check the task status to see if it's finished
                    celery_task = AsyncResult(task_id, app=celery_app) # Re-initialize celery_task here

                    try:
                        celery_task_ready = celery_task.ready()
                        logger.info(f"Task {task_id}: Current status {celery_task.state}")
                        # Log status updates less frequently
                        if celery_task_ready:
                            logger.info(f"Task {task_id}: Completed with status {celery_task.state}")

                        # Generate status response
                        response = create_task_status_response(celery_task, task, task_id, base_url)
                        data = f"data: {json.dumps(response)}\n\n"  # Note the double newline for SSE format
                        
                        
                        yield data.encode('utf-8')  # Ensure we're yielding bytes
                        
                        # Exit when the task is complete
                        if celery_task_ready:
                            break
                            
                    except Exception as e:
                        # Handle any serialization errors
                        error_msg = f"Error generating status response: {str(e)}"
                        logger.error(error_msg)
                        yield f"data: {json.dumps({'error': error_msg})}\n".encode('utf-8')
                        
                    # Wait before checking again
                    await asyncio.sleep(1)
                
                # Send the [DONE] marker to end the stream
                yield b"data: [DONE]\n\n"
                
            except Exception as e:
                # TODO: Handle exceptions in the streaming loop IN the frontend
                logger.error(f"Fatal error in status stream: {str(e)}", exc_info=True)
                yield f"event: error\ndata: {json.dumps({'error': 'A fatal error occurred while streaming the task status.', 'fatal': True})}\n\n".encode('utf-8')
                yield b"data: [DONE]\n\n"

        return StreamingResponse(
            stream_task_status(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable proxy buffering
                "X-Stream-Status": "active",
            }
        )
    except Exception as e:
        # Return a proper error response instead of letting the exception bubble up
        logger.exception("Error setting up status stream for task %s: %s", task_id, e)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "error": "Failed to stream task status due to an internal error."
            }
        )

async def create_new_task(
    redis: Redis,
    input_path: str,
    query: str,
    schema: Optional[str],
    cache: str,
    base_url: str,
    config: dict
) -> JSONResponse:
    """Create and initialize a new task."""
    decoded_url = unquote(input_path)
    if not decoded_url.startswith(('http://', 'https://')):
        decoded_url = 'https://' + decoded_url

    # Enqueue the task to Celery
    task = llm_extraction_task.delay(decoded_url, query, schema, cache)
    task_id = task.id

    await redis.hset(f"task:{task_id}", values={
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "created_at": datetime.now(timezone.utc).isoformat(),
        "url": decoded_url,
    })

    return JSONResponse({
        "task_id": task_id,
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "url": decoded_url,
        "_links": {
            "self": {"href": f"{base_url}llm/{task_id}"},
            "status": {"href": f"{base_url}llm/{task_id}"}
        }
    })



# process_llm_extraction is now a Celery task in tasks.py, so it's removed from here.
