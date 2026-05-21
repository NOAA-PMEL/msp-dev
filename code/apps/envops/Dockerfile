FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Create the envops user and group
RUN groupadd envops
RUN useradd -m -g envops -s /bin/bash envops

ENV APP_HOME /app
WORKDIR $APP_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY VERSION .
COPY envops_app.py ./envops_app.py
COPY ./main.py main.py
COPY ./pages ./pages
COPY ./assets ./assets

# Give ownership to the envops user
RUN chown -R envops:envops $APP_HOME
USER envops

EXPOSE 8080

# Call uvicorn directly
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]