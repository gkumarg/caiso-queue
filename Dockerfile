FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY scripts/ ./scripts/

# Copy data directories
COPY raw/ ./raw/
COPY data/ ./data/
COPY reports/ ./reports/

# Default command: run the parser (which also invokes analysis via workflows)
ENTRYPOINT ["python", "scripts/parse_queue.py"]
