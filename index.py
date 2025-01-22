import asyncio
from pathlib import Path
import sys
from typing import Annotated  # noqa: F401
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import StreamingResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from firestore import db
import uvicorn

# Use uvloop for enhanced performance
# from crawl import on_browser_created
from actions import infinite_scroll, load_more  # noqa: F401
from api_tokens import verify_token
from crwal import process_scheduled_tasks, reader, worker
from triggers import event_stream
import uvloop  # type: ignore


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))


scheduler = AsyncIOScheduler()


# Add jobs to the scheduler
@scheduler.scheduled_job("interval", minutes=1)
async def scheduled_job():
    await process_scheduled_tasks()


async def startup_event():
    scheduler.start()
    asyncio.create_task(worker())


# Shutdown event to clean up resources
async def shutdown_event():
    scheduler.shutdown()


# Initialize FastAPI app
app = FastAPI(on_startup=[startup_event], on_shutdown=[shutdown_event])


# Store connected WebSocket clients
clients = set()


@app.get("/user/data")
async def get_user_data(decoded_token=Depends(verify_token)):
    user_id = decoded_token.get("uid")
    user_ref = db.collection("users").document(user_id)
    user_data = user_ref.get().to_dict()
    if user_data:
        return user_data
    else:
        return {"message": "No data found for user"}


@app.websocket("/ws/events")
async def websocket_endpoint(
    websocket: WebSocket, decoded_token: bool = Depends(verify_token)
):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await asyncio.sleep(1)  # Example: periodic event
            await websocket.send_text("Hello, this is a server event!")
    except WebSocketDisconnect:
        clients.remove(websocket)
        print("Client disconnected")


@app.post("/events")
async def stream(request: Request, decoded_token: bool = Depends(verify_token)):
    """Event stream endpoint."""
    try:
        data = await request.json()
        url = data.get("url")
        return StreamingResponse(event_stream(url), media_type="text/event-stream")
    except HTTPException as e:
        # Handle specific HTTP exceptions here if needed
        return {"error": str(e.detail)}


@app.post("/crawl")
async def crawl(
    request: Request, response: Response, decoded_token: bool = Depends(verify_token)
):
    """Reader endpoint."""
    try:
        return await reader(request, response)
    except HTTPException as e:
        # Handle specific HTTP exceptions here if needed
        return {"error": str(e.detail)}


@app.get("/")
async def root(decoded_token: bool = Depends(verify_token)):
    return {
        "message": "WebSocket server is running. Connect to /ws/events or /events for stream-events"
    }


if __name__ == "__main__":
    uvicorn.run(
        "index:app",
        host="0.0.0.0",
        port=8000,
        loop="uvloop",
        reload=True,
        workers=4,  # or any other value that makes sense for your use case
    )
