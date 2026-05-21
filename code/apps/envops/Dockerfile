FROM ghcr.io/noaa-pmel/msp/envds-base:latest

RUN groupadd envops && useradd -m -g envops -s /bin/bash envops

ENV APP_HOME=/app
WORKDIR $APP_HOME

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY VERSION .
COPY envops_app.py main.py ./
COPY pages/ ./pages/
COPY assets/ ./assets/   

RUN chown -R envops:envops $APP_HOME
USER envops

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--root-path", "/msp/envops"]