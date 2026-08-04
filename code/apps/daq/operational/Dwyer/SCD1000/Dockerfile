# FROM python:3.10-slim
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
# # RUN pip3 install envds
# # RUN pip install testapp
# RUN pip3 install --no-index --find-links=/lib/envds envds
# lib/envds-0.1.0-py3-none-any.whl

# for fastapi/uvicorn
# COPY . .
# without fastapi/uvicorn
COPY ./scd1000.py .



CMD ["python", "scd1000.py", "--host", "0.0.0.0"]
