# from fastapi import FastAPI, BackgroundTasks
# from pydantic import BaseModel
# from typing import List, Dict
# from upstash_redis import Redis
# import json
# import uuid
# import time
# from firebase_admin import firestore, initialize_app
# import firebase_admin


from datetime import datetime
from functools import partial
import json
import logging
import os
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import unquote
from uuid import uuid4
from courlan import get_base_url
from crawl4ai import AsyncWebCrawler, BM25ContentFilter, BrowserConfig, CacheMode, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, LLMConfig, LLMContentFilter, LLMExtractionStrategy, LXMLWebScrapingStrategy, MemoryAdaptiveDispatcher, PruningContentFilter, RateLimiter
from crawl4ai.utils import perform_completion_with_backoff

from fastapi import HTTPException, Request,status
from fastapi.responses import JSONResponse
import psutil
import time
from datetime import datetime # Import datetime for Celery task ID generation
from celery.result import AsyncResult # Import AsyncResult

from upstash_redis.asyncio import Redis

from crawler_pool import cancel_crawler
from utils import FilterType, TaskStatus, _get_memory_mb, create_task_response, decode_redis_hash, is_task_id, should_cleanup_task
from celery_app import celery_app # Import celery_app
from tasks import crawl_stream_task, crawl_task, llm_extraction_task # Import Celery tasks

logger = logging.getLogger(__name__)

    

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
        browser_config = BrowserConfig.load(browser_config)
        # browser_config.verbose = True # Set to False or remove for production stress testing
        browser_config.verbose = False
        crawler_config = CrawlerRunConfig.load(crawler_config)
        crawler_config.scraping_strategy = LXMLWebScrapingStrategy()
        crawler_config.stream = True

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            )
        )

        from crawler_pool import get_crawler
        bcrawler:tuple[AsyncWebCrawler, str] = await get_crawler(browser_config)
        crawler, _ = bcrawler
        # crawler = AsyncWebCrawler(config=browser_config)
        # await crawler.start()

        results_gen = await crawler.arun_many(
            urls=urls,
            config=crawler_config,
            dispatcher=dispatcher
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
        "created_at": datetime.utcnow().isoformat(),
        "urls": json.dumps(urls),
        "result": "",
        "error": "",
    })
    return {"task_id": task_id}

async def handle_crawl_stream_job(
        redis: Redis,
        base_url: str,
        urls: List[str],
        browser_config: Dict,
        crawler_config: Dict,
        config: Dict,
) -> JSONResponse:
    
    """ Fire-and-forget version of handle_stream_crawl_request.
    Creates a task in Redis, runs the heavy work in a background task and publish to redis pub/sub,
    lets /crawl/job/{task_id} polling fetch the result by websockets """

    logger.info(f"Enqueuing a streaming crawl job for URLs: {urls}")
    # print crawler_config
    logger.info(f"Crawler config: {crawler_config}")

    # Enqueue the task to Celery
    task = crawl_stream_task.delay(urls, browser_config, crawler_config)
    task_id = task.id


    # Store initial task details in Redis, signature will be updated by worker
    await redis.hset(f"task:{task_id}", values={
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "created_at": datetime.utcnow().isoformat(),
        "urls": json.dumps(urls),
        "result": "",
        "error": "",
    })

    return JSONResponse({
        "task_id": task_id,
        "status": TaskStatus.IN_PROGRESS.value, # Convert Enum to string
        "_links": {
            "self": {"href": f"{base_url}crawl/stream/job/{task_id}"},
            "status": {"href": f"{base_url}crawl/stream/job/{task_id}"}
        }
    })



async def cancel_a_job(redis: Redis, task_id: str, force: bool = False):

    """Cancel a running crawl job using Celery's revocation."""
    
    task_info = await redis.hgetall(f"task:{task_id}")

    if not task_info:
        raise HTTPException(status_code=404, detail="Task not found")

    try:
        browser_sign = task_info["signature"] if hasattr(task_info, "no_signature") else None
        if (browser_sign and browser_sign != "no_signature"):
            await cancel_crawler(browser_sign)  # Remove the crawler to free resources

        # Revoke the Celery task
        import signal
        celery_app.control.revoke(
            task_id,
            terminate=True,
            signal=signal.SIGKILL if force else signal.SIGTERM if os.name == 'posix' else signal.SIGTERM
        )
        

        await redis.hset(f"task:{task_id}", 
                         values={
                             "status": TaskStatus.CANCELED.value, 
                             "update_at": datetime.utcnow().isoformat()
                            })
        return {"message": f"Task {task_id} canceled successfully."}
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
    task = await redis.hgetall(f"task:{task_id}")
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    celery_task = AsyncResult(task_id, app=celery_app)
    task = decode_redis_hash(task)
    response = create_task_response(celery_task, task, task_id, base_url)

    if task["status"] in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED]:
        if not keep and should_cleanup_task(task["created_at"]):
            await redis.delete(f"task:{task_id}")

    return JSONResponse(response)


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
        "created_at": datetime.now().isoformat(),
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
