import httpx


async def stream_events(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()  # Optional: check for HTTP errors
        message_count = 0  # Initialize a counter for messages
        async for line in response.aiter_lines():
            if line:  # Ensure line is not empty
                message_count += 1  # Increment the counter
                print(message_count)
                # Process the incoming data here (you could print or store it)
                # print(line)  # Print the received message

url = "http://localhost:8000/crawl/stream/job/{task_id}"

if __name__ == "__main__":
    import asyncio
    task_id = "5490bc7e-37c5-4573-a0fa-c54c16620ded"  # Replace with your actual task ID
    asyncio.run(stream_events(url.format(task_id=task_id)))