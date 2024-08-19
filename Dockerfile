FROM python:3.9-slim-bullseye

WORKDIR /app

# Install the required packages
RUN apt-get update && apt-get -y upgrade
RUN apt-get -y --fix-missing install cron python3 python3-pip postgresql-client

# Activate the virtual environment and install dependencies
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy your application code
COPY .env .

# Install dependencies:
COPY requirements.txt .

# Install the required packages
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

# Create the log directory
RUN mkdir -p /app/logs

# Cron jobs
# NOTE: Change the schedule as needed
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
