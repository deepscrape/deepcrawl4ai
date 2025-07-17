# Scalable Web Scraping Service

## What does this server do?

This server is a high-performance, distributed web scraping and crawling platform designed for AI-powered data extraction at scale. It provides:

- **Concurrent scraping of multiple URLs** using Playwright and FastAPI
- **Distributed job queueing** with Celery and Redis/Upstash
- **Real-time job status tracking** and resource monitoring
- **Dynamic rate limiting and backpressure** to adapt to system load
- **API endpoints** for submitting scrape jobs, checking status, and managing operations
- **Metrics and observability** via Prometheus integration
- **Scalable deployment** on Fly.io with multi-process and multi-worker support
- **Support for screenshots, PDFs, and JavaScript execution** via API
- **Adaptive queue management and error tracking** for robust operation

The server is suitable for large-scale, production-grade web crawling, data collection, and AI-driven content extraction tasks.

## Overview

A high-performance, distributed web scraping service built with:

- Python
- FastAPI
- Playwright
- Upstash Redis
- Fly.io Deployment

## Features

- Concurrent scraping of multiple URLs
- Distributed job queueing
- Scalable architecture
- Real-time job status tracking

## Prerequisites

- Python 3.11+
- Upstash Redis Account
- Fly.io Account

## Local Development Setup

1. Clone the repository
2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set Upstash Redis Environment Variables:

    ```bash
    export UPSTASH_REDIS_REST_URL=your_redis_url
    export UPSTASH_REDIS_REST_TOKEN=your_redis_token
    ```

4. Run API Server:

    ```bash
    uvicorn server:app --reload
    ```

## Deployment

```bash
fly launch
```

## API Endpoints

- `POST /scrape` — Submit a scraping job (with URL(s), options)
- `POST /llm/job` — Submit an LLM extraction job
- `GET /user/data` — Get user data (requires authentication)
- `POST /config/dump` — Evaluate and dump config
- `GET /ws/events` — WebSocket endpoint for real-time events
- `GET /llm/job/{task_id}` — Get LLM job status/result
- `GET /crawl/job/{task_id}` — Get crawl job status/result
- `POST /crawl/job/{task_id}/cancel` — Cancel a running crawl job
- `GET /metrics` — Prometheus metrics endpoint
- `GET /health` — Health check endpoint

## Configuration

Adjust concurrency and worker settings in `worker.py`
