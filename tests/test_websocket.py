import asyncio
import logging
import pytest
import websockets

WS_URL = "ws://localhost:8000/ws/events"
logger = logging.getLogger(__name__)
@pytest.mark.asyncio
async def test_websocket_events():
    async with websockets.connect(WS_URL) as websocket:
        # Wait for a message from the server
        msg = await websocket.recv()
        logger.info(f"Received message: {msg}")
        if isinstance(msg, bytes):
            msg_str = msg.decode("utf-8")
        elif isinstance(msg, (bytearray, memoryview)):
            msg_str = bytes(msg).decode("utf-8")
        else:
            msg_str = str(msg)
        assert "server event" in msg_str
        # Optionally, send a message to the server and check response
        # await websocket.send("ping")
        # response = await websocket.recv()
        # assert response == "pong"  # If your server echoes or responds

@pytest.mark.asyncio
async def test_websocket_no_close():
    websocket = await websockets.connect(WS_URL)
    try:
        # Wait for a message from the server
        msg = await websocket.recv()
        if isinstance(msg, bytes):
            msg_str = msg.decode("utf-8")
        elif isinstance(msg, (bytearray, memoryview)):
            msg_str = bytes(msg).decode("utf-8")
        else:
            msg_str = str(msg)
        assert "server event" in msg_str
        # Optionally, send a message to the server and check response
        # await websocket.send("ping")
        # response = await websocket.recv()
        # assert response == "pong"  # If your server echoes or responds
        await asyncio.sleep(120)  # Keep the connection alive for 1 second
    finally:
        await websocket.close()