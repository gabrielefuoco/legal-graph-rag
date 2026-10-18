FROM python:3.11-slim

# Standard environment variables for Python in Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# PyTorch index: CUDA wheels on GPU hosts, CPU wheels on GPU-less ones.
# Override it from the build args (see docker-compose.yml / .env).
ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cu121

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir torch torchvision torchaudio --index-url ${TORCH_INDEX_URL}

# Copy the rest of the application
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Default command to check environment
CMD ["python", "manage.py", "--help"]
