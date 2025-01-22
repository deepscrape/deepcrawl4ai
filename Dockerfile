# Use an official Python runtime as a base image
FROM python:3.10-slim AS base

# Use the Playwright runtime as a secondary stage
FROM zenika/alpine-chrome:with-playwright AS playwright

# Switch back to the base image
FROM base

# Set the working directory inside the container
WORKDIR /app

# Set environment variables
ENV NODE_ENV=production

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Install necessary system dependencies for Playwright
# RUN apt-get update && apt-get install -y \
#     libnss3 libxss1 libasound2 libatk1.0-0 libpangocairo-1.0-0 \
#     libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0 \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# https://playwright.dev/docs/browsers
# Install only the required Playwright browser(s)
RUN playwright install --with-deps chromium

# Expose the port your FastAPI app will run on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "index:app", "--host", "0.0.0.0", "--port", "8000"]
