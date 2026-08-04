FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED=True

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application logic and XML templates
COPY ./sidecar.py .
COPY ./templates ./templates

# Run the sidecar service
CMD ["python", "sidecar.py"]