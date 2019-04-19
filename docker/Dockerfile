# EMAIL SENDING SERVICE
# Linux distribution DEBIAN 9 - Stretch
FROM python:3.7.0-stretch
MAINTAINER Rares Dragomir <rares_dr@yahoo.com>

# Install MISC
RUN apt-get update
RUN apt-get install -y cron

# Install project's python dependencies
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Add crontab file in the cron directory
ADD ./script/crontab /etc/cron.d/email-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/email-cron

# Apply cron job
RUN crontab /etc/cron.d/email-cron

# Create the service directory
RUN mkdir -p /mailer
WORKDIR /mailer

COPY ./script/start.sh /start.sh
RUN chmod a+rx /*.sh

# Run script on container start
ENTRYPOINT ["/start.sh"]
