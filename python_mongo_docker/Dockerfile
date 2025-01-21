#Dockerfile
FROM python:3.9-slim

# Install system dependencies including Git
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./app .

# Setup git configuration script
COPY ./scripts/setup-git.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/setup-git.sh

# Make sure scripts are executable
RUN chmod +x ./entrypoint.sh

# Set the entrypoint script
ENTRYPOINT ["./entrypoint.sh"]