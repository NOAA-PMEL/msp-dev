# FROM python:3.11-slim
# FROM python:3.10
FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED=True

# Copy local code to the container image.
ENV APP_HOME=/app
WORKDIR $APP_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
# COPY lib/. /lib
# RUN pip3 install envds
# RUN pip install testapp
# RUN pip3 install --no-index --find-links=/lib/envds envds
# lib/envds-0.1.0-py3-none-any.whl

# COPY . .
COPY ./datastore.py .
COPY ./datastore_requests.py .
COPY ./main.py .
COPY ./db_client.py .
COPY ./redis_client.py .
# WORKDIR $APP_HOME/envds
# COPY ./apis ./apis
# COPY ./registry.py ./registry.py

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# CMD ["uvicorn", "envds.main:app", "--host", "0.0.0.0", "--port", "8080"]
CMD ["python", "datastore.py", "--host", "0.0.0.0"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
