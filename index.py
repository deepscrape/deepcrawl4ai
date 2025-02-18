import asyncio

import os
from pathlib import Path
import sys

# from typing import Annotated  # noqa: F401
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

from firestore import db
import uvicorn

# from crawl import on_browser_created
# from actions import infinite_scroll, load_more  # noqa: F401
from api_tokens import verify_token
from crawl import reader
from triggers import event_stream

# Use uvloop for enhanced performance
import uvloop  # type: ignore

# Set the number of workers
NUM_WORKERS = os.getenv("NUM_WORKERS", os.cpu_count())
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))


# Initialize FastAPI app on_startup=[startup_event], on_shutdown=[shutdown_event]
app = FastAPI()


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
        workers=NUM_WORKERS,  # or any other value that makes sense for your use case
    )
