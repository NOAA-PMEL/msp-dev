FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED=True

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application logic
COPY ./main.py .

# Run the FastAPI service using Uvicorn
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]