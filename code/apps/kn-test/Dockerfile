# FROM python:3.10-slim
FROM python:3.12-slim-bullseye

# WORKDIR /app
ENV APP_HOME /app
WORKDIR $APP_HOME

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# COPY VERSION .
COPY ./kn_test.py ./app.py

# CMD [ "python3", "verify.py" ]
# CMD exec gunicorn --workers 1 --threads 8 --timeout 0 app:app
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
