# Use a lightweight Python base image
FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Set environment variables to prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . /app/

# Expose the standard port for Knative services
EXPOSE 8080

# Run the application via the main.py entrypoint
CMD ["python", "main.py"]