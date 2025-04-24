FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y procps gcc g++ make curl liblz4-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    minio \
    pandas \
    pyarrow>=7.0.0 \
    requests \
    trino \
    python-dotenv \
    pyyaml \
    matplotlib \
    great-expectations

# Set working directory
WORKDIR /scripts

# Keep container running
CMD ["tail", "-f", "/dev/null"]