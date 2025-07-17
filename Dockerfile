# Use an official Python runtime as a base image
FROM python:3.12-slim-bookworm AS build

# Set environment variables to production
ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    DEBIAN_FRONTEND=noninteractive \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYTHON_ENV=production 

ARG APP_HOME=/app
ARG PYTHON_VERSION=3.12
ARG INSTALL_TYPE=default
ARG ENABLE_GPU=false
ARG TARGETARCH


# Add Maintainer Info
LABEL maintainer="Prokopis Antoniadis"
LABEL description="🔥🕷️ Crawl4AI: LLM Web Crawler & scraper"
LABEL version="1.0"

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     curl \
#     wget \
#     gnupg \
#     cmake \
#     pkg-config \
#     python3-dev \
#     libjpeg-dev \
#     supervisor \
#     && apt-get clean \ 
#     && rm -rf /var/lib/apt/lists/*

# Install minimal system dependencies for Playwright and Python
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    libnss3 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxrandr2 \
    libdrm2 \
    libgbm1 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    curl fonts-liberation ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# RUN apt-get update && apt-get dist-upgrade -y \
#     && rm -rf /var/lib/apt/lists/*

# RUN if [ "$ENABLE_GPU" = "true" ] && [ "$TARGETARCH" = "amd64" ] ; then \
#     apt-get update && apt-get install -y --no-install-recommends \
#     nvidia-cuda-toolkit \
#     && apt-get clean \ 
#     && rm -rf /var/lib/apt/lists/* ; \
# else \
#     echo "Skipping NVIDIA CUDA Toolkit installation (unsupported platform or GPU disabled)"; \
# fi

# RUN if [ "$TARGETARCH" = "arm64" ]; then \
#     echo "🦾 Installing ARM-specific optimizations"; \
#     apt-get update && apt-get install -y --no-install-recommends \
#     libopenblas-dev \
#     && apt-get clean \ 
#     && rm -rf /var/lib/apt/lists/*; \
# elif [ "$TARGETARCH" = "amd64" ]; then \
#     echo "🖥️ Installing AMD64-specific optimizations"; \
#     apt-get update && apt-get install -y --no-install-recommends \
#     libomp-dev \
#     && apt-get clean \ 
#     && rm -rf /var/lib/apt/lists/*; \
# else \
#     echo "Skipping platform-specific optimizations (unsupported platform)"; \
# fi

# Create a non-root user and group
# RUN groupadd -r appuser && useradd --no-log-init -r -g appuser appuser

# Create and set permissions for appuser home directory
# RUN mkdir -p /home/appuser && chown -R appuser:appuser /home/appuser

# Set the working directory inside the container
WORKDIR ${APP_HOME}

# Copy the requirements file and install Python dependencies
# Copy supervisor config first (might need root later, but okay for now)
# COPY supervisord.conf .

COPY requirements.txt .
COPY config.yml .
COPY .env .

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir playwright

RUN pip install --no-cache-dir --upgrade pip && \
    python -c "import crawl4ai; print('✅ crawl4ai is ready to rock!')" && \
    python -c "from playwright.sync_api import sync_playwright; print('✅ Playwright is feeling dramatic!')"


# RUN crawl4ai-setup

# https://playwright.dev/docs/browsers
# Install only the required Playwright browser (Chromium)
RUN playwright install chromium

# RUN playwright install --with-deps

RUN crawl4ai-doctor

# RUN mkdir -p /home/appuser/.cache/ms-playwright \
#     && cp -r /root/.cache/ms-playwright/chromium-* /home/appuser/.cache/ms-playwright/ \
#     && chown -R appuser:appuser /home/appuser/.cache/ms-playwright

# Copy the application code into the container
COPY . .
# Change ownership of the application directory to the non-root user
# RUN chown -R appuser:appuser ${APP_HOME}

# Switch to the non-root user before starting the application
# USER appuser

# # Install only the required Playwright browser (Chromium)
# RUN playwright install chromium

# Expose the port your FastAPI app will run on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]

# Start the application using supervisord
# CMD ["supervisord", "-c", "supervisord.conf"]