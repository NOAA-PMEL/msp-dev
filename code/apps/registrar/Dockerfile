# FROM python:3.10-slim
FROM python:3.12-slim-bullseye

RUN groupadd  registrar
RUN useradd -m -g registrar -s /bin/bash registrar

# WORKDIR /app
ENV APP_HOME /app
WORKDIR $APP_HOME

# RUN apt update && apt -y install xmlstarlet

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY VERSION .

# COPY datasets.d.sh .
# RUN chmod +x datasets.d.sh

COPY ./registrar.py ./app.py

# run the same as tomcat so we can update the erddap datasets for now
RUN chown -R registrar:registrar $APP_HOME
USER registrar

# CMD [ "python3", "verify.py" ]
# CMD exec gunicorn --workers 1 --threads 8 --timeout 0 app:app
CMD ["python", "app.py", "--host", "0.0.0.0"]
