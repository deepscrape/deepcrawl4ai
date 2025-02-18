from crawl import process_scheduled_tasks, worker
from monitor import DynamicRateLimiter, WorkerMonitor
from apscheduler.schedulers.asyncio import AsyncIOScheduler


scheduler = AsyncIOScheduler()

app = {}


# Add jobs to the scheduler
@scheduler.scheduled_job("interval", minutes=0.3, max_instances=10)
async def scheduled_job():
    monitor: WorkerMonitor = app.worker_monitor
    await monitor.update_metrics()

    # Only process if system is healthy
    if await should_process_tasks(monitor.metrics):
        await process_scheduled_tasks()
    else:
        print("System under load, deferring scheduled tasks")


async def should_process_tasks(metrics):
    return all(
        [
            metrics["cpu_usage"] < 85,
            metrics["memory_usage"] < 85,
            metrics["error_rate"] < 0.15,
        ]
    )


async def exception_handler(exc):
    # Handle exceptions here
    print(f"Exception in worker process: {exc}")


async def startup_event():
    """
    Key Improvements:

    Dynamic rate limiting based on system metrics
    Per-machine monitoring and coordination
    Adaptive queue length limits
    Error rate tracking and backoff
    System health checks before processing

    """
    scheduler.start()

    # Initialize monitoring
    app.state.worker_monitor = WorkerMonitor(0)
    app.state.rate_limiter = DynamicRateLimiter()

    # worker_args = []

    # worker_args = [i for i in range(NUM_WORKERS)]
    # for i in range(NUM_WORKERS):
    #     asyncio.create_task(worker(i))

    await worker()
    # async with Pool(
    #     loop_initializer=uvloop.new_event_loop, exception_handler=exception_handler
    # ) as pool:
    #     async for results in pool.map(worker, worker_args):
    #         print(await results)
    #         pass  # Handle results if necessary


# Shutdown event to clean up resources
async def shutdown_event():
    scheduler.shutdown()
