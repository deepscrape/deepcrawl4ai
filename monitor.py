# Resource Monitor Class
import asyncio
import json
import os
import socket
import time
import uuid
import firebase_admin.firestore
import psutil
from kombu import Queue

from redisCache import redis
from schemas import OperationResult, SystemStats
from utils import TaskStatus, retry_async

MAX_QUEUE_LENGTH = 180
RATE_LIMIT_TTL = 60  # 1 minute
RATE_LIMIT_COUNT = 75  # 75 operations per minute
WAITING_TIME = 0.1  # 5 seconds


class ResourceMonitor:
    def __init__(self, num_cores=None):
        self.memory_threshold = 0.85  # 85%
        self.cpu_threshold = 0.90  # 90%
        self.error_threshold = 0.15  # 15%
        self.num_cores = num_cores or psutil.cpu_count()

    async def check_resources(self):
        memory_usage = psutil.virtual_memory().percent / 100
        cpu_usage = psutil.cpu_percent() / 100
        per_core_usage = psutil.cpu_percent(percpu=True)

        return {
            "memory_ok": memory_usage < self.memory_threshold,
            "cpu_ok": cpu_usage < self.cpu_threshold,
            "memory_usage": memory_usage,
            "cpu_usage": cpu_usage,
            "per_core_usage": per_core_usage,
        }

    async def get_core_metrics(self):
        cores_metrics = []
        for core_id, core_usage in enumerate(psutil.cpu_percent(percpu=True)):
            core_metrics = {
                "core_id": core_id,
                "cpu_usage": core_usage / 100,
                "memory_usage": psutil.virtual_memory().percent / 100,
            }
            cores_metrics.append(core_metrics)
        return cores_metrics

# Worker Pool Management
class WorkerMonitor:
    def __init__(self, worker_id, db = None, queue_name=Queue("celery").name):
        fly_machine_id = str(os.environ.get("FLY_ALLOC_ID", socket.gethostname()))
        self.machine_id = f"{fly_machine_id}:process:{str(worker_id)}"
        self.queue_name = queue_name

        self.firestore_client = db
        self.metrics = SystemStats(
            cpu_usage=0,
            memory_usage=0,
            queue_length=0,
            error_rate=0,
        )

    async def get_operation_queue_len(self):
        # Get the length of the operation queue
        queue_length = await redis.llen(self.queue_name)
        return queue_length

    async def record_operation_start(self, operation_id, start_time):
        await redis.hset(
            f"operation_metrics:{operation_id}",
            values={
                "start_time": start_time,
                "machine_id": self.machine_id,
                "status": TaskStatus.IN_PROGRESS.value,
            },
        )
    async def record_operation_abort(self, operation_id, duration, status=TaskStatus.CANCELED.value):
        await redis.hset(
            f"operation_metrics:{operation_id}",
            values={
                "duration": duration,
                "status": status,
            },
        )

    async def record_metrics(self, operation_metrics: OperationResult):
        operation_id = operation_metrics.operation_id

        # Store in redis for real-time monitoring
        await retry_async(redis.hset, f"operation_metrics:{operation_id}", values=operation_metrics.model_dump(exclude={"operation_id"}))

        # Store in Firestore for historical analysis
        if operation_metrics.status == TaskStatus.COMPLETED.value:
            
            await self.record_operation()
            if not self.firestore_client:
                return
            
            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            await retry_async(run_in_executor, doc_ref.set, {
                "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                "duration": operation_metrics.duration,
                "memory_used": operation_metrics.memory_used,
                "urls_processed": operation_metrics.urls_processed,
                "machine_id": self.machine_id,
            })
        elif operation_metrics.status == TaskStatus.FAILED.value:

            # Update the operation status in redis
            await self.record_error(operation_metrics.error)

            if not self.firestore_client:
                return

            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            await retry_async(run_in_executor, doc_ref.set, {
                "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                "error": operation_metrics.error,
                "duration": operation_metrics.duration,
                "status": operation_metrics.status,
                "machine_id": self.machine_id,
            })
        elif operation_metrics.status == TaskStatus.CANCELED.value:
            
            # Update the operation status in redis
            await retry_async(redis.hset, f"operation_metrics:{operation_id}", values={"status": TaskStatus.CANCELED.value})

            if not self.firestore_client:
                return

            doc_ref = self.firestore_client.collection("operation_metrics").document(
                operation_id
            )
            await retry_async(run_in_executor, doc_ref.set, {
                "timestamp": firebase_admin.firestore.firestore.SERVER_TIMESTAMP,
                "status": TaskStatus.CANCELED.value,
                "duration": operation_metrics.duration,
                "urls_processed": operation_metrics.urls_processed,
                "machine_id": self.machine_id,
            })

        # Example: Firestore batch write for high-volume operations
        # batch = db.batch()
        # for doc_ref, doc_data in docs_to_write:
        #     batch.set(doc_ref, doc_data)
        # batch.commit()
        # This is much faster and more reliable for bulk updates than individual writes.

    async def record_error(self, error):
        # Record error in redis
        await redis.hincrby("record_error", self.machine_id, 1)
        print(f"Error recorded for machine {self.machine_id}: {error}")

    async def record_operation(self):
        # Record error in redis
        await redis.hincrby("total_operations", self.machine_id, 1)

    async def get_error_rate(self):
        # Calculate error rate based on operation metrics
        error_count = await redis.hget("record_error", self.machine_id)
        total_operations = await redis.hget("total_operations", self.machine_id)
        if error_count and total_operations:
            return int(error_count) / int(total_operations)
        return 0

    async def update_metrics(self):
        """
        Asynchronously updates the system metrics for the current machine.
        This method gathers the current CPU usage, memory usage, queue length, and error rate,
        then updates the internal metrics attribute with these values. It also stores the metrics
        in a Redis hash for external monitoring or aggregation.
        Side Effects:
            - Updates self.metrics with the latest SystemStats.
            - Persists the metrics to Redis under the key "machine_metrics:{machine_id}".
        Raises:
            Any exceptions raised by the underlying async methods or Redis operations.
        """
        queue_length = await self.get_operation_queue_len()
        systats = SystemStats(
                cpu_usage=psutil.cpu_percent(),
                memory_usage=psutil.virtual_memory().percent,
                queue_length=queue_length,
                error_rate=await self.get_error_rate(),
            )
        self.metrics = systats
        
        # values for upstash, mapping for redis io lab
        await redis.hset(f"machine_metrics:{self.machine_id}", values=self.metrics.model_dump())


class DynamicRateLimiter:
    def __init__(self, base_limit=100):
        """
        Initialize the dynamic rate limiter with a base limit.

        Args:
            base_limit (int): The base rate limit. Defaults to 100.
        """
        self.base_limit = base_limit
        self.resource_monitor = ResourceMonitor()

    async def calculate_rate_limit(self, metrics=None):
        """
        Calculate the dynamic rate limit based on system metrics.

        Args:
            metrics (dict): A dictionary containing system metrics.
                - cpu_usage (float): CPU usage percentage.
                - memory_usage (float): Memory usage percentage.
                - error_rate (float): Error rate.

        Returns:
            int: The calculated dynamic rate limit.
        """
        if not metrics:
            resources = await self.resource_monitor.check_resources()
            metrics = {
                "cpu_usage": resources["cpu_usage"] * 100,
                "memory_usage": resources["memory_usage"] * 100,
                "error_rate": 0.05,  # Default error rate
            }

        # Adaptive rate limit calculation Adjust rate based on system metrics
        cpu_factor = 1 - (metrics["cpu_usage"] / 100)
        memory_factor = 1 - (metrics["memory_usage"] / 100)
        error_factor = 1 - metrics["error_rate"]

        dynamic_limit = self.base_limit * min(cpu_factor, memory_factor, error_factor)
        return max(int(dynamic_limit), 10)  # Never go below 10


# self.machine_id = f"{fly_machine_id}worker:{str(worker_id)}"

async def run_in_executor(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))