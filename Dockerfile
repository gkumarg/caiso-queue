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
COPY dashboard/ ./dashboard/
COPY run.py ./run.py

# Make sure Python can find the modules
ENV PYTHONPATH="${PYTHONPATH}:/app:/app/dashboard"

# Environment variables
ENV SMTP_HOST=
ENV SMTP_USER=
ENV SMTP_PASS=
ENV NOTIFICATION_EMAIL=

# Volume configuration
VOLUME ["/app/raw", "/app/data", "/app/reports"]

# Expose port for Streamlit
EXPOSE 8501

# Create an entrypoint script to handle multiple commands
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# Create helper script for debugging Python module imports
RUN echo '#!/bin/python\nimport sys\nimport os\nprint("Python version:", sys.version)\nprint("Python path:", sys.path)\nprint("Working directory:", os.getcwd())\nprint("Directory contents:", os.listdir("."))\nprint("Dashboard contents:", os.listdir("dashboard") if os.path.exists("dashboard") else "Not found")' > /app/debug_env.py \
    && chmod +x /app/debug_env.py
RUN chmod +x ./entrypoint.sh

# Default command: run the dashboard
ENTRYPOINT ["./entrypoint.sh"]
CMD ["dashboard"]
