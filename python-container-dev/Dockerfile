# Use the official lightweight Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies and ffmpeg in one step to reduce image layers
RUN apt-get update && apt-get install -y --no-install-recommends \
    mkvtoolnix \
    ffmpeg curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Copy requirements.txt to the container
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into the container
COPY . /app

# Expose port 5000 for the Flask app
EXPOSE 5000

# Default command to run the application
CMD ["python", "app.py"]
