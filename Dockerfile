FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
# gcc and python3-dev are often needed for lxml and other C-extensions
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Keep the container running for development
CMD ["tail", "-f", "/dev/null"]
