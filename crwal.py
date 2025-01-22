import asyncio
from datetime import datetime
import json
from fastapi import Request, Response

from crawlstore import getCrawlMetadata, updateCrawlOperation
from triggers import (
    arrayBuffer_basic_crawl,
    basic_crawl_operation,
    event_stream,
    json_basic_crawl,
)
from redis import redis

""" 
To determine how many operations you can run, let's calculate the memory usage per operation and then divide the total available memory by the memory usage per operation.

Given that:

10 URLs task have a total memory usage of 100MB
Each operation (or task) has a similar memory usage pattern
The total available memory on each machine is 1GB (1024MB)
We can calculate the memory usage per operation as follows:

Memory usage per operation = Total memory usage / Number of operations = 100MB / 10 operations = 10MB per operation

Now, let's calculate how many operations you can run on a single machine with 1GB of memory:

Number of operations per machine = Total available memory / Memory usage per operation = 1024MB (1GB) / 10MB per operation = 102.4 operations per machine

Since you have 2 machines, each with 1GB of memory, you can run a total of:

Total number of operations = Number of operations per machine x Number of machines = 102.4 operations per machine x 2 machines = 204.8 operations

So, approximately 205 operations can be run simultaneously on your 2-machine cluster with 1GB of memory each, assuming each operation has a similar memory usage pattern.

However, to be safe and account for any potential memory spikes or overhead, you might want to consider reducing this number by 10-20% to ensure your machines don't run out of memory. This would put the estimated number of operations at around 164-184 operations. 
"""
MAX_QUEUE_LENGTH = 180
RATE_LIMIT_TTL = 60  # 1 minute
RATE_LIMIT_COUNT = 75  # 75 operations per minute
WAITING_TIME = 5  # 5 seconds

# Create a separate queue for scheduled tasks
scheduled_queue = "scheduled_operation_queue"


async def reader(request: Request, response: Response) -> Response:
    try:
        # Get request headers and data
        headers = request.headers
        data = await request.json()

        # Set default URL
        url = data.get("urls")

        # Check Accept header
        if "Accept" not in headers:
            return Response(
                "Missing Accept header in request",
                media_type="text/plain",
                status_code=403,
            )

        accept_header = headers["Accept"]

        response_content_types = {
            "application/json": {"Content-Type": "application/json"},
            "text/plain": {"Content-Type": "text/plain"},
            "application/octet-stream": {"Content-Type": "application/octet-stream"},
            "text/event-stream": {"Content-Type": "text/event-stream"},
        }

        # Check if Accept header is supported
        for supported_header, response_config in response_content_types.items():
            if supported_header in accept_header:
                response.headers["Content-Type"] = response_config["Content-Type"]
                response.status_code = 200
                response.media_type = response_config["Content-Type"]

                if response_config["Content-Type"] == "application/json":
                    response.body = await json_basic_crawl(url)
                elif (
                    response_config["Content-Type"] == "text/plain"
                    or response_config["Content-Type"] == "application/octet-stream"
                ):
                    response.body = await arrayBuffer_basic_crawl(url)
                elif response_config["Content-Type"] == "text/event-stream":
                    response.body = await event_stream(url)

                return response

        # Return 406 Not Acceptable if Accept header is not supported
        return Response(
            b"Unsupported Accept header", media_type="application/json", status_code=406
        )

    except Exception as e:
        # Handle exceptions
        print(e)
        return Response(
            b"Internal Server Error", media_type="application/json", status_code=500
        )


async def worker():
    """
    urls: string[];
    modelAI?: AIModel;
    authorId: string;
    created_At: number;
    schedule_At?: number;
    sumPrompt: string;
    name: string
    status: CrawlOperationStatus
    color: string
    metadata?: CrawlConfig

    """
    while True:
        try:
            # Check if the queue has reached the maximum length
            queue_length = await redis.llen("operation_queue")
            if queue_length > MAX_QUEUE_LENGTH:
                print(
                    f"Queue has reached maximum length: {MAX_QUEUE_LENGTH}. Waiting..."
                )
                await asyncio.sleep(WAITING_TIME)
                continue

            # Check if the rate limit has been exceeded
            if await redis.exists("rate_limit") == 1:
                print("Rate limit exceeded. Waiting...")
                await asyncio.sleep(WAITING_TIME)
                continue

            task_data = await redis.rpop("operation_queue")
            if not task_data:
                print("Queue is empty. Retrying...")
                await asyncio.sleep(WAITING_TIME)
                continue

            task = json.loads(task_data)
            operation_id = task["operationId"]
            uid = task["authorId"]

            # Update Firestore with results
            print(f"Processing operation: {operation_id}")

            markdown = await processOperation(operation_id, uid, task, task_data)

            if markdown is not None:
                await updateCrawlOperation(
                    uid,
                    operation_id,
                    {
                        "status": "Completed",
                        "markdown": markdown,
                    },
                )
                print(f"Completed operation: {operation_id}")
            else:
                await asyncio.sleep(WAITING_TIME)

            # Update the rate limit
            await redis.incr("rate_limit")
            await redis.expire("rate_limit", RATE_LIMIT_TTL)

        except Exception as e:
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": "Failed",
                    "error": str(e),
                },
            )
            print(f"Error processing task: {e}")
            await asyncio.sleep(WAITING_TIME)


async def processOperation(operation_id, uid, task, task_data):
    scheduled_at = task.get("scheduled_At", None)  # or any other default value
    status = task["status"]
    urls: list[str] = task["urls"]
    url: str = urls.pop(0)
    metadataId = task["metadataId"]
    metadata = None
    markdown: str = None

    if metadataId:
        metadata = await getCrawlMetadata(metadataId, uid)

    # Check the status of the task
    if status == "Start":
        # Update status to In Progress before processing
        await updateCrawlOperation(
            uid,
            operation_id,
            {
                "status": "In Progress",
                "error": None,
            },
        )
        # If the status is Start, process the task immediately
        markdown = await basic_crawl_operation(url)
    elif status == "Scheduled":
        print(metadata)
        print("Scheduled")
        # If the status is Scheduled, check if the scheduled time is in the past
        scheduled_at_dt = datetime.fromtimestamp(scheduled_at / 1000)
        now = datetime.now()
        if now >= scheduled_at_dt:
            # If the scheduled time is in the past, process the task immediately
            # Update status to In Progress before processing
            await updateCrawlOperation(
                uid,
                operation_id,
                {
                    "status": "In Progress",
                    "error": None,
                },
            )
            markdown = await basic_crawl_operation(url)
        else:
            # If the scheduled time is in the future, push the task back into the scheduled queue
            await schedule_task(task_data, scheduled_at)
            print(f"Task {operation_id} is scheduled for {scheduled_at_dt}. Waiting...")
            return markdown
    else:
        print(f"Unknown status: {status}")

    return markdown


async def schedule_task(task, scheduled_at):
    # Add the task to the scheduled queue with the scheduled time
    await redis.zadd(scheduled_queue, {task: scheduled_at})


async def process_scheduled_tasks():
    while True:
        # Get the current time
        now = datetime.now().timestamp() * 1000

        # Get tasks that are ready to be processed from the scheduled queue
        tasks = await redis.zrangebyscore(scheduled_queue, 0, now)

        # Process each task
        for task in tasks:
            # Remove the task from the scheduled queue
            await redis.zrem(scheduled_queue, task)

            # Add the task to the operation queue
            await redis.rpush("operation_queue", task)

        # Wait for 1 minute before checking again
        # await asyncio.sleep(RATE_LIMIT_TTL)
