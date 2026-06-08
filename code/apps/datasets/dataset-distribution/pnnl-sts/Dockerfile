FROM python:3.11-slim

WORKDIR /app

# Install basic system utilities if needed by the PNNL transfer binary later 
# (e.g., curl or openssh if it uses scp/sftp internally)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Pre-create the absolute path required by the PNNL STS binary
RUN mkdir -p /datasets/beacons/sts && chmod -R 777 /datasets/beacons/sts

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run Uvicorn on port 8080 to match our Knative containerPort configurations
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]