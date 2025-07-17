import asyncio
from datetime import datetime
import json
import logging
import os
from functools import partial # Import partial
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlResult, CrawlerRunConfig, DefaultMarkdownGenerator, LLMConfig, LLMContentFilter, LLMExtractionStrategy, LXMLWebScrapingStrategy, MemoryAdaptiveDispatcher, PruningContentFilter, RateLimiter
from crawl4ai.utils import perform_completion_with_backoff
from fastapi import HTTPException, status
import psutil


from redisCache import redis

from celery_app import celery_app
from celery.exceptions import SoftTimeLimitExceeded
from utils import FilterType, TaskStatus, decode_redis_hash, is_task_id, setup_logging, should_cleanup_task, load_config
from crawler_pool import get_crawler, cancel_crawler

config = load_config()
setup_logging(config)
logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def crawl_task(self, urls: List[str], browser_config: Dict, crawler_config: Dict) :
    """Celery task to handle crawl requests."""
    import asyncio
    logger.info(f"Starting crawl task with URLs: {urls}")
    # logger.info(f"Browser config: {browser_config}")
    # logger.info(f"Crawler config: {crawler_config}")

    try:
        # # Periodically check for revocation
        # if self.is_aborted():
        #         # await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
        #         return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}
        
       
        # return {"status": TaskStatus.COMPLETED, "message": "Crawl task completed successfully."}
        # async def runner():
        #     import time
        #     data = "No URLs provided"
        #     for i in range(10):  # Long-running task
        #         time.sleep(1)
        #         print(f"Crawling {data}, step {i}")
        #     return f"Crawled {data}"
        
        asyncio.run(_crawl_task_impl(self, urls, browser_config, crawler_config))

        return {"status": TaskStatus.COMPLETED, "message": "Crawl task completed successfully."}

    except SoftTimeLimitExceeded:
        return {"status": "CANCELED", "message": "Task exceeded time limit and was aborted."}
    except Exception as e:
        logger.error(f"Error in crawl task: {e}")
        return {"status": TaskStatus.FAILED, "message": str(e)}

@celery_app.task(bind=True)
async def llm_extraction_task(self, url: str, instruction: str, schema: Optional[str] = None, cache: str = "0") -> Dict:
    """Celery task to process LLM extraction."""
    task_id = self.request.id

    try:
        # Check for cancellation
        if self.request.aborted:
            await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
            return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}

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
            schema=json.loads(schema) if schema else None, # Corrected schema handling
        )

        cache_mode = CacheMode.ENABLED if cache == "1" else CacheMode.WRITE_ONLY

        async with AsyncWebCrawler() as crawler:
            result: CrawlResult = await crawler.arun( # Explicitly type result
                url=url,
                config=CrawlerRunConfig(
                    extraction_strategy=llm_strategy,
                    scraping_strategy=LXMLWebScrapingStrategy(),
                    cache_mode=cache_mode
                )
            )

        if not result.success:
            await redis.hset(f"task:{task_id}", values={
                "status": TaskStatus.FAILED.value, # Convert Enum to string
                "error": result.error_message or "" # Ensure string
            })
            return {"success": False, "error": result.error_message}

        try:
            content = json.loads(result.extracted_content) if result.extracted_content else None # Handle None
        except json.JSONDecodeError:
            content = result.extracted_content
        
        await redis.hset(f"task:{task_id}", values={
            "status": TaskStatus.COMPLETED.value, # Convert Enum to string
            "result": json.dumps(content) if content is not None else "" # Ensure string
        })
        return {"success": True, "result": content}

    except Exception as e:
        logger.error(f"Celery LLM extraction task error: {str(e)}", exc_info=True)
        await redis.hset(f"task:{task_id}", values={
            "status": TaskStatus.FAILED,
            "error": str(e)
        })
        raise


async def _crawl_task_impl(self, urls: List[str], browser_config: Dict, crawler_config: Dict):
    """Internal implementation of the crawl task."""
    task_id = self.request.id
    logger.info(f"Celery task {task_id} started.")
    
    browser = BrowserConfig.load(browser_config)
    crawler, sign = await get_crawler(browser) # get_crawler returns crawler and signature
    
    try:
        # Check for cancellation
        # if self.request.aborted:
        #     await redis.hset(f"task:{task_id}", values={"status": TaskStatus.CANCELED})
        #     return {"status": TaskStatus.CANCELED, "message": "Task aborted by user."}

        await redis.hset(f"task:{task_id}", values={
            "signature": sign if sign else "no_signature"  # Store signature if available
        })

        results, total_time, mem_delta_mb, peak_mem_mb = await handle_crawl_request(
            urls=urls,
            crawler=crawler,
            crawler_config=crawler_config,
            config=config,
        )

        crawled = {
            "success": True,
            "results": "",
            "server_processing_time_s": total_time,
            "server_memory_delta_mb": mem_delta_mb,
            "server_peak_memory_mb": peak_mem_mb
        }
        # Check if result is a dict
        if isinstance(crawled, Dict):
            logger.info(f"Result is a dict with keys: {list(crawled.keys())}")
        else:
            logger.info(f"Result type: {type(crawled)}")

        await redis.hset(f"task:{task_id}", values={
            "signature": sign if sign else "no_signature",  # Store signature if available
            "status": TaskStatus.COMPLETED,
            "result": json.dumps(crawled),  # Ensure result is a string
            "update_at": datetime.utcnow().isoformat()  # Store update time
        })

        await cancel_crawler(sign)  # Remove the crawler to free resources

        await asyncio.sleep(5)  # Give Redis time to process the update

        # return {"success": True, "results": crawled}

    except Exception as e:
        logger.error(f"Celery crawl task error: {str(e)}", exc_info=True)
        await redis.hset(f"task:{task_id}", values={
            "status": TaskStatus.FAILED,
            "error": str(e),
        })
        await cancel_crawler(sign)  # Remove the crawler to free resources
        raise


async def handle_crawl_request(
    urls: List[str],
    crawler: AsyncWebCrawler,
    crawler_config: dict,
    config: dict
) -> tuple:
    """Handle non-streaming crawl requests."""
    start_mem_mb = _get_memory_mb() # <--- Get memory before
    start_time = time.time()
    mem_delta_mb = None
    peak_mem_mb = start_mem_mb
    try:
        urls = [('https://' + url) if not url.startswith(('http://', 'https://')) else url for url in urls]

        crawler_conf = CrawlerRunConfig.load(crawler_config)

        dispatcher = MemoryAdaptiveDispatcher(
            memory_threshold_percent=config["crawler"]["memory_threshold_percent"],
            rate_limiter=RateLimiter(
                base_delay=tuple(config["crawler"]["rate_limiter"]["base_delay"])
            ) if config["crawler"]["rate_limiter"]["enabled"] else None
        )
        

        base_config = config["crawler"]["base_config"]
        # Iterate on key-value pairs in global_config then use haseattr to set them 
        for key, value in base_config.items():
            if hasattr(crawler_conf, key):
                setattr(crawler_conf, key, value)

        results = []
        func = getattr(crawler, "arun" if len(urls) == 1 else "arun_many")
        partial_func = partial(func, 
                                urls[0] if len(urls) == 1 else urls, 
                                config=crawler_conf, 
                                dispatcher=dispatcher)
        
        results = await partial_func()

        # await crawler.close()
        
        end_mem_mb = _get_memory_mb() # <--- Get memory after
        end_time = time.time()
        total_time = end_time - start_time
        
        if start_mem_mb is not None and end_mem_mb is not None:
            mem_delta_mb = end_mem_mb - start_mem_mb # <--- Calculate delta
            peak_mem_mb = max(peak_mem_mb if peak_mem_mb else 0, end_mem_mb) # <--- Get peak memory
            logger.info(f"Memory usage: Start: {start_mem_mb} MB, End: {end_mem_mb} MB, Delta: {mem_delta_mb} MB, Peak: {peak_mem_mb} MB, Total Time: {total_time}" )
                              
        return results, total_time, mem_delta_mb, peak_mem_mb

    except Exception as e:
        logger.error(f"Crawl error: {str(e)}", exc_info=True)
        if 'crawler' in locals() and crawler.ready: # Check if crawler was initialized and started
             try:
                 await crawler.close()
             except Exception as e:
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

# --- Helper to get memory ---
def _get_memory_mb():
    try:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception as e:
        logger.warning(f"Could not get memory info: {e}")
        return None