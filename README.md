# Scalable Web Scraping Service

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

- `POST /scrape`: Submit scraping job
- `GET /operation/{operation_id}`: Check job status

## Configuration

Adjust concurrency and worker settings in `worker.py`
