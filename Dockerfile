FROM python:3.11-slim

# Standard environment variables for Python in Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

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

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Default command to check environment
CMD ["python", "manage.py", "--help"]
