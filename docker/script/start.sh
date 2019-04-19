#!/bin/bash

# Create the log file to be able to run tail
touch /var/log/scheduler.log
touch /var/log/stats.log

# Run cron and tail follow cron log
cron start && tail -f /var/log/scheduler.log