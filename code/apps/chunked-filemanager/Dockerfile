FROM ghcr.io/noaa-pmel/msp/envds-base:latest

ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# Using uvicorn to serve the health check API while the background task runs
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]