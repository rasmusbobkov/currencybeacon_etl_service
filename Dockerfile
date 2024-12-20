# Start from a lightweight Python image
FROM python:3.11-slim

# Create a user in the Docker image
RUN useradd -ms /bin/bash appuser
USER appuser
WORKDIR /app

RUN mkdir /app/data && chown appuser:appuser /app/data

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the files
COPY main.py .
COPY schema.sql .
COPY queries.sql .
COPY README.md .

# Copy .env if available (not required if you pass env variables at runtime)
# ENV variables can also be passed in via docker run or docker-compose
# COPY .env .  # Uncomment if you place a .env file in the build context

# Default command to run ETL when container starts
CMD [ "python", "main.py" ]
