# from fastapi import FastAPI, BackgroundTasks
# from pydantic import BaseModel
# from typing import List, Dict
# from upstash_redis import Redis
# import json
# import uuid
# import time
# from firebase_admin import firestore, initialize_app
# import firebase_admin


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
