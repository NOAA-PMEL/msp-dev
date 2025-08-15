# FROM python:3.10-slim
# FROM python:3.10
FROM --platform=$BUILDPLATFORM ghcr.io/noaa-pmel/msp/envds-base:latest

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Declare TARGETARCH as a build argument in the local scope
ARG TARGETARCH

RUN apt-get update && apt-get install -y unzip wget && apt-get install -y libusb-1.0-0 && apt-get install udev -y

ARG LJ_FILE=LabJack-LJM_2025-05-07.zip
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        echo "Building for ARM64" ; \
        # ARM64 specific commands
        wget "https://files.labjack.com/installers/LJM/Linux/AArch64/release/${LJ_FILE}" ; \
    elif [ "$TARGETARCH" = "amd64" ]; then \
        echo "Building for AMD64" ; \
        # AMD64 specific commands
        wget "https://files.labjack.com/installers/LJM/Linux/x64/release/${LJ_FILE}" ; \
    fi

RUN unzip ./${LJ_FILE}
RUN ./labjack_ljm_installer.run -- --without-kipling --no-restart-device-rules

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
# COPY ./LabJack_Tx_interface_definition.json .
COPY ./tx.py .
COPY ./labjack_client.py .


# WORKDIR $APP_HOME/envds
# COPY ./apis ./apis
# COPY ./registry.py ./registry.py

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# CMD ["uvicorn", "envds.main:app", "--host", "0.0.0.0", "--port", "8080"]
CMD ["python", "tx.py", "--host", "0.0.0.0"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
