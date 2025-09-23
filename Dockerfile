# Build stage with UV
FROM ghcr.io/astral-sh/uv:0.8.22 AS uv

# Builder stage
FROM python:3.12-slim AS builder

# Set build-time environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_INSTALLER_METADATA=1 \
    UV_LINK_MODE=copy

ARG APP_HOME=/app
WORKDIR ${APP_HOME}

# Install build dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gcc \
    g++ \
    python3-dev \
    pkg-config \
    libjpeg-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Use UV to build wheels
RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt

# Final stage
FROM python:3.12-slim

# Metadata
LABEL maintainer="Prokopis Antoniadis" \
      description="🔥🕷️ Crawl4AI: LLM Web Crawler & scraper" \
      version="0.1.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PYTHON_ENV=production \
    UV_LINK_MODE=copy \
    DISPLAY=:99

# Set build arguments
ARG APP_HOME=/app

# Install UV in final stage
COPY --from=uv /uv /usr/local/bin/uv
RUN chmod +x /usr/local/bin/uv

WORKDIR ${APP_HOME}

# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-liberation \
    ca-certificates \
    lsof \
    # Add build dependencies for madoka
    build-essential \ 
    curl \
    gcc \ 
    g++ \ 
    python3-dev \
    pkg-config \
    libjpeg-dev \
    cmake \
    wget \
    gnupg \
    supervisor \
    # Playwright system dependencies
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libxcursor1 \
    libxss1 \
    libgtk-3-0 \
    xvfb \
    x11vnc \
    git \
    # Add sudo for X11 management
    && git clone --depth 1 https://github.com/novnc/noVNC /opt/noVNC \
    && git clone --depth 1 https://github.com/novnc/websockify /opt/noVNC/utils/websockify \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/*

# Create non-root user
RUN groupadd -r appuser && \
    useradd --no-log-init -r -g appuser appuser && \
    mkdir -p /home/appuser/.cache /ms-playwright && \
    chown -R appuser:appuser /home/appuser /ms-playwright ${APP_HOME}

# Install Python dependencies using UV
# COPY --from=builder /app/wheels /wheels

# Copy dependencies and install
COPY --from=builder ${APP_HOME}/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt && \
    uv pip install --system playwright && \
    playwright install --with-deps chromium

# Verify installations
RUN python -c "import crawl4ai; print('✅ crawl4ai is ready to rock!')" && \
    python -c "from playwright.sync_api import sync_playwright; print('✅ Playwright is feeling dramatic!')"

# Copy application code
COPY --chown=appuser:appuser . .
COPY --chown=appuser:appuser config.yml .

# Set display environment variable
# ENV DISPLAY=:99

# Run diagnostics
RUN crawl4ai-doctor

# Expose ports
EXPOSE 8000 9222 6080

# Healthcheck dont need, fly io do this for us
# HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
#     CMD curl -f http://localhost:8000/health || exit 1

# Copy and set permissions for the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh && \
    chown root:root /usr/local/bin/docker-entrypoint.sh && \
    ls -la /usr/local/bin/docker-entrypoint.sh  # Verify permissions

# Switch to non-root user
USER appuser

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Start application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--ws", "websockets"]
