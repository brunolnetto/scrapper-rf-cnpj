FROM python:3.9-slim-bullseye

WORKDIR /app

# Copy your application code
COPY .env .

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install the required packages
RUN apt-get update
RUN apt-get -y install cron python3 python3-pip postgresql-client

# Create the log directory
RUN mkdir -p /app/logs

# Cron jobs
RUN echo '* * * * * python3 /app/src/main.py >> /app/logs/cron.log 2>&1' > cron-config

# Apply cron job
RUN crontab cron-config

# Create the log file to be able to run tail
RUN touch /app/logs/cron.log

# Run the command on container startup
CMD echo "starting" && \
    echo "continuing" && \
    (cron) && \
    echo "tailing..." && \
    : >> /app/logs/cron.log && \
    tail -f /app/logs/cron.log
