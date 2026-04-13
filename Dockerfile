# Multi-stage Dockerfile for PySpark Big Data Learning
FROM python:3.11-slim AS base

# Set working directory
WORKDIR /app

# Install system dependencies required by PySpark and Java
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    wget \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Development stage
FROM base AS development

# Copy source code
COPY src/ ./src/
COPY notebooks/ ./notebooks/
COPY data/ ./data/

# Expose Jupyter port
EXPOSE 8888

# Expose Spark UI ports
EXPOSE 4040

# Default command: start Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

# Production stage
FROM base AS production

# Copy only necessary files
COPY src/ ./src/
COPY data/ ./data/

# Entry point for running scripts
ENTRYPOINT ["python"]
CMD ["src/spark_processor.py"]
