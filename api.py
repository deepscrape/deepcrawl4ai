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
from datetime import datetime
from functools import partial
import json
import logging
import os
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from urllib.parse import unquote
from uuid import uuid4
from courlan import get_base_url
from crawl4ai import AsyncWebCrawler, BM25ContentFilter, BrowserConfig, CacheMode, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, LLMConfig, LLMContentFilter, LLMExtractionStrategy, LXMLWebScrapingStrategy, MemoryAdaptiveDispatcher, PruningContentFilter, RateLimiter
from fastapi import BackgroundTasks, HTTPException, Request,status
from fastapi.responses import JSONResponse
import psutil
import time

from upstash_redis.asyncio import Redis

from utils import FilterType, TaskStatus, create_task_response, decode_redis_hash, is_task_id, should_cleanup_task

logger = logging.getLogger(__name__)


# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None
    
# # Initialize Firebase if not already initialized
# if not firebase_admin._apps:
#     initialize_app()

# app = FastAPI()
# redis = Redis.from_env()
# firestore_client = firestore.client()


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
    background_tasks: BackgroundTasks,
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
                        "href": f"{base_url}/llm/{input_path}?q=Extract+main+content",
                        "title": "Try this example"
                    }
                }
            })

        return await create_new_task(
            redis,
            background_tasks,
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
    config: Optional[dict] = None 
) -> tuple[list[str], float | None, Any | None, Any | int | None]:
    """Handle markdown generation requests."""
    start_mem_mb = _get_memory_mb() # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    try:
        urls = [('https://' + unquote(url)) if not url.startswith(('http://', 'https://')) else url for url in urls]
        print(f"urls: {urls}")
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
        

        browser_config = BrowserConfig(headless=True)
        from crawler_pool import get_crawler
        crawler = await get_crawler(browser_config)
        # async with AsyncWebCrawler(config=BrowserConfig()) as crawler:
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


        return (
            [result.markdown.raw_markdown if filter_type == FilterType.RAW
            else result.markdown.fit_markdown for result in results],            
            total_time,
            mem_delta_mb,
            peak_mem_mb
        )

    except Exception as e:
        logger.error(f"Markdown error: {str(e)}", exc_info=True)
        if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
            #  try:
            #      await crawler.close()
            #  except Exception as close_e:
            #       logger.error(f"Error closing crawler during exception handling: {close_e}")
            logger.error(f"Error closing crawler markdowns during exception handling: {str(e)}")

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

async def handle_crawl_job(
    redis,
    background_tasks: BackgroundTasks,
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
    task_id = f"crawl_{uuid4().hex[:8]}"
    await redis.hset(f"task:{task_id}", mapping={
        "status": TaskStatus.PROCESSING,         # <-- keep enum values consistent
        "created_at": datetime.utcnow().isoformat(),
        "url": json.dumps(urls),                 # store list as JSON string
        "result": "",
        "error": "",
    })

    async def _runner():
        try:
            result = await handle_crawl_request(
                urls=urls,
                browser_config=browser_config,
                crawler_config=crawler_config,
                config=config,
            )
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.COMPLETED,
                "result": json.dumps(result),
            })
            await asyncio.sleep(5)  # Give Redis time to process the update
        except Exception as exc:
            await redis.hset(f"task:{task_id}", mapping={
                "status": TaskStatus.FAILED,
                "error": str(exc),
            })

    background_tasks.add_task(_runner)
    return {"task_id": task_id}

async def handle_crawl_request(
    urls: List[str],
    browser_config: dict,
    crawler_config: dict,
    config: dict
) -> dict:
    """Handle non-streaming crawl requests."""
    start_mem_mb = _get_memory_mb() # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    
    try:
        urls = [('https://' + url) if not url.startswith(('http://', 'https://')) else url for url in urls]
        browser_config = BrowserConfig.load(browser_config)
        crawler_config = CrawlerRunConfig.load(crawler_config)

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            ) if config["crawler"]["rate_limiter"]["enabled"] else None
        )
        
        from crawler_pool import get_crawler
        crawler = await get_crawler(browser_config)

        # crawler: AsyncWebCrawler = AsyncWebCrawler(config=browser_config)
        # await crawler.start()
        
        base_config = config["crawler"]["base_config"]
        # Iterate on key-value pairs in global_config then use haseattr to set them 
        for key, value in base_config.items():
            if hasattr(crawler_config, key):
                setattr(crawler_config, key, value)

        results = []
        func = getattr(crawler, "arun" if len(urls) == 1 else "arun_many")
        partial_func = partial(func, 
                                urls[0] if len(urls) == 1 else urls, 
                                config=crawler_config, 
                                dispatcher=dispatcher)
        results = await partial_func()

        # await crawler.close()
        
        end_mem_mb = _get_memory_mb() # <--- Get memory after
        end_time = time.time()
        
        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb) # <--- Get peak memory
        logger.info(f"Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB")
                              
        return {
            "success": True,
            "results": [result.model_dump() for result in results],
            "server_processing_time_s": end_time - start_time,
            "server_memory_delta_mb": mem_delta_mb,
            "server_peak_memory_mb": peak_mem_mb
        }

    except Exception as e:
        logger.error(f"Crawl error: {str(e)}", exc_info=True)
        if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
            #  try:
            #      await crawler.close()
            #  except Exception as close_e:
            #       logger.error(f"Error closing crawler during exception handling: {close_e}")
            logger.error(f"Error closing crawler during exception handling: {str(e)}")

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

    task = decode_redis_hash(task)
    response = create_task_response(task, task_id, base_url)

    if task["status"] in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
        if not keep and should_cleanup_task(task["created_at"]):
            await redis.delete(f"task:{task_id}")

    return JSONResponse(response)


async def create_new_task(
    redis: Redis,
    background_tasks: BackgroundTasks,
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

    from datetime import datetime
    task_id = f"llm_{int(datetime.now().timestamp())}_{id(background_tasks)}"
    
    await redis.hset(f"task:{task_id}", values={
        "status": TaskStatus.PROCESSING,
        "created_at": datetime.now().isoformat(),
        "url": decoded_url
    })

    background_tasks.add_task(
        process_llm_extraction,
        redis,
        config,
        task_id,
        decoded_url,
        query,
        schema,
        cache
    )

    return JSONResponse({
        "task_id": task_id,
        "status": TaskStatus.PROCESSING,
        "url": decoded_url,
        "_links": {
            "self": {"href": f"{base_url}/llm/{task_id}"},
            "status": {"href": f"{base_url}/llm/{task_id}"}
        }
    })


async def process_llm_extraction(
    redis: Redis,
    config: dict,
    task_id: str,
    url: str,
    instruction: str,
    schema: Optional[str] = None,
    cache: str = "0"
) -> None:
    """Process LLM extraction in background."""
    try:
        # If config['llm'] has api_key then ignore the api_key_env
        api_key = ""
        if "api_key" in config["llm"]:
            api_key = config["llm"]["api_key"]
        else:
            api_key = os.environ.get(config["llm"].get("api_key_env", None), "")
        llm_strategy = LLMExtractionStrategy(
            llm_config=LLMConfig(
                provider=config["llm"]["provider"],
                api_token=api_key
            ),
            instruction=instruction,
            schema=json.loads(schema) if schema else None,
        )

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY

        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    extraction_strategy=llm_strategy,
                    scraping_strategy=LXMLWebScrapingStrategy(),
                    cache_mode=cache_mode
                )
            )

        if not result.success:
            await redis.hset(f"task:{task_id}", values={
                "status": TaskStatus.FAILED,
                "error": result.error_message
            })
            return

        try:
            content = json.loads(result.extracted_content)
        except json.JSONDecodeError:
            content = result.extracted_content
        await redis.hset(f"task:{task_id}", values={
            "status": TaskStatus.COMPLETED,
            "result": json.dumps(content)
        })

    except Exception as e:
        logger.error(f"LLM extraction error: {str(e)}", exc_info=True)
        await redis.hset(f"task:{task_id}", values={
            "status": TaskStatus.FAILED,
            "error": str(e)
        })