FROM ghcr.io/noaa-pmel/msp/envds-base:latest

ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./kld20s.py .

CMD ["python", "kld20s.py", "--host", "0.0.0.0"]