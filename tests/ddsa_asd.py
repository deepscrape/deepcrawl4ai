# import asyncio
# import logging
# from typing import List, Dict, Any
# from playwright.async_api import async_playwright
# from upstash_redis import Redis
# import json
# import time
# import crawl4ai
# from firebase_admin import firestore, initialize_app
# import firebase_admin
# from apscheduler.schedulers.asyncio import AsyncIOScheduler


# class ScraperWorker:
#     def __init__(self, redis_url: str = None, max_concurrent_tabs: int = 10):
#         # Initialize Firebase if not already initialized
#         if not firebase_admin._apps:
#             initialize_app()

#         self.firestore_client = firestore.client()
#         self.redis = Redis.from_env() if not redis_url else Redis(url=redis_url)
#         self.max_concurrent_tabs = max_concurrent_tabs
#         self.logger = logging.getLogger(__name__)
#         logging.basicConfig(level=logging.INFO)

#         # Initialize scheduler for job management
#         self.scheduler = AsyncIOScheduler()

#     async def process_operation(self, operation: Dict[str, Any]):
#         """
#         Process a single scraping operation with multiple URLs using crawl4ai

#         :param operation: Dictionary containing scraping job details
#         :return: Processed results or None if failed
#         """
#         start_time = time.time()
#         try:
#             results = []
#             for url in operation.get("urls", []):
#                 try:
#                     # Use crawl4ai for enhanced scraping
#                     scrape_result = await crawl4ai.async_crawl(
#                         url,
#                         playwright=True,
#                         timeout=30,  # 30 second timeout
#                     )

#                     # Store result in Firestore
#                     doc_ref = self.firestore_client.collection(
#                         "scrape_results"
#                     ).document()
#                     doc_ref.set(
#                         {
#                             "url": url,
#                             "content": scrape_result.text,
#                             "metadata": scrape_result.metadata,
#                             "timestamp": firestore.SERVER_TIMESTAMP,
#                         }
#                     )

#                     results.append(
#                         {
#                             "url": url,
#                             "content_length": len(scrape_result.text),
#                             "timestamp": time.time(),
#                             "firestore_doc_id": doc_ref.id,
#                         }
#                     )

#                 except Exception as e:
#                     self.logger.error(f"Error scraping {url}: {e}")
#                     results.append({"url": url, "error": str(e)})

#             # Store results in Redis
#             operation_result = {
#                 "id": operation.get("id"),
#                 "results": results,
#                 "total_time": time.time() - start_time,
#             }

#             await self.redis.rpush("completed_operations", json.dumps(operation_result))
#             return operation_result

#         except Exception as e:
#             self.logger.error(f"Operation failed: {e}")
#             await self.redis.rpush(
#                 "failed_operations",
#                 json.dumps({"operation": operation, "error": str(e)}),
#             )
#             return None

#     async def worker(self):
#         """
#         Continuous worker that pulls operations from Redis queue
#         """
#         while True:
#             try:
#                 # Blocking pop from pending operations queue
#                 raw_operation = await self.redis.blpop("pending_operations", timeout=30)

#                 if not raw_operation:
#                     await asyncio.sleep(1)
#                     continue

#                 operation = json.loads(raw_operation[1])
#                 self.logger.info(f"Processing operation: {operation.get('id')}")

#                 await self.process_operation(operation)

#             except Exception as e:
#                 self.logger.error(f"Worker error: {e}")
#                 await asyncio.sleep(5)

#     async def start_workers(self, worker_count: int = 5):
#         """
#         Start multiple concurrent workers

#         :param worker_count: Number of concurrent workers to start
#         """
#         # Start the scheduler
#         self.scheduler.start()

#         # Create worker pool
#         workers = [self.worker() for _ in range(worker_count)]
#         await asyncio.gather(*workers)


# async def main():
#     worker = ScraperWorker()
#     await worker.start_workers()


# if __name__ == "__main__":
#     asyncio.run(main())
