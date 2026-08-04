FROM ghcr.io/noaa-pmel/msp/envds-base:latest

# Create a non-root user for security
RUN groupadd envops && useradd -m -g envops -s /bin/bash envops

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install dependencies [cite: 2]
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy version info and the application code
COPY VERSION .
COPY main.py ./       
COPY app.py ./        
COPY pages/ ./pages/
COPY assets/ ./assets/   

# Ensure the non-root user owns the application directory
RUN chown -R envops:envops $APP_HOME
USER envops

# Expose the port (matches the uvicorn command)
EXPOSE 8080

# Start the FastAPI server (which mounts the Dash app and handles WebSockets)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--root-path", "/envds/envops"]