#!/bin/bash

# The command we want to monitor
PROCESS_CMD="python3 -u /home/ec2-user/agent/agent.py"
# The rerun command
RERUN_CMD="sudo -u ec2-user python3 -u /home/ec2-user/agent/agent.py >> /home/ec2-user/agent/run.log 2>&1 &"

# Check if the process is running
PROCESS_ID=$(pgrep -f "$PROCESS_CMD")

if [ -z "$PROCESS_ID" ]; then
    # If the process is not running, restart it
    eval "$RERUN_CMD"
    echo "$(date) Process rerun." >> /home/ec2-user/agent/monitor.log
else
    echo "$(date) Process is already running with PID: $PROCESS_ID" >> /home/ec2-user/agent/monitor.log
fi