FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for mounted volumes
RUN mkdir -p /app/raw /app/data /app/reports

# Copy source code
COPY scripts/ ./scripts/

# Environment variables
ENV SMTP_HOST=
ENV SMTP_USER=
ENV SMTP_PASS=
ENV NOTIFICATION_EMAIL=

# Volume configuration
VOLUME ["/app/raw", "/app/data", "/app/reports"]

# Default command: run the complete pipeline
ENTRYPOINT ["python", "scripts/run_pipeline.py"]
