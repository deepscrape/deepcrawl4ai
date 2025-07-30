import pytest
import httpx
import json

@pytest.mark.asyncio
async def test_event_stream_crawl():
    """
    Tests the event_stream function by simulating a client request
    with Accept: "text/event-stream" and a POST body.
    """
    test_url = "https://openai.com/api/pricing/"
    base_url = "http://localhost:8000"
    endpoint = "/crawl"

    headers = {
        "Accept": "text/event-stream",
        "Content-Type": "application/json",
    }
    payload = {
        "urls": test_url,
        "priority": 10
    }

    async with httpx.AsyncClient(base_url=base_url) as client:
        print(f"Sending POST request to {base_url}{endpoint} with URL: {test_url}")
        async with client.stream("POST", endpoint, headers=headers, json=payload, timeout=60.0) as response:
            response.raise_for_status()
            assert response.headers["content-type"] == "application/x-ndjson"

            full_content = ""
            async for chunk in response.aiter_bytes():
                decoded_chunk = chunk.decode("utf-8")
                print(f"Received chunk: {decoded_chunk}")
                full_content += decoded_chunk
                # Assert that each chunk starts with "data: " and ends with "\n\n"
                assert decoded_chunk.startswith("data: ")
                assert decoded_chunk.endswith("\n\n")

                # Parse the JSON payload if it's not the [DONE] message
                if "[DONE]" not in decoded_chunk:
                    json_data_str = decoded_chunk[len("data: "):-2]
                    try:
                        event_data = json.loads(json_data_str)
                        assert "id" in event_data
                        assert "object" in event_data
                        assert "created" in event_data
                        assert "model" in event_data
                        assert "choices" in event_data
                        if event_data["choices"]:
                            assert "delta" in event_data["choices"][0]
                            assert "content" in event_data["choices"][0]["delta"]
                    except json.JSONDecodeError as e:
                        pytest.fail(f"Failed to decode JSON from chunk: {json_data_str}. Error: {e}")
                else:
                    print("Received [DONE] signal.")

            assert "[DONE]" in full_content
            print("Stream test completed successfully.")
