FROM ghcr.io/noaa-pmel/msp/envds-base:latest

ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application logic
COPY ./main.py .

# Run the GitOps reconciliation loop
CMD ["python3", "main.py"]