#!/bin/bash
echo "setup script start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
# Log path
LOG_FILE="/home/ec2-user/agent/setup.log"

install_pip3() {
    echo "Start install pip3..." | tee -a $LOG_FILE
    sudo yum install -y python3-pip | tee -a $LOG_FILE
}

check_pip3() {
    if command -v pip3 &> /dev/null
    then
        echo "pip3 install successfully!" | tee -a $LOG_FILE
        return 0
    else
        echo "pip3 install failed!" | tee -a $LOG_FILE
        return 1
    fi
}

while true; do
    install_pip3
    if check_pip3; then
        break
    else
        echo "Retry to install pip3..." | tee -a $LOG_FILE
    fi
done

echo "pip3 install successfully and checked!" | tee -a $LOG_FILE

sudo -u ec2-user pip3 install -r requirements.txt
echo "requirements install successfully " | tee -a $LOG_FILE

# Start agent
sudo -u ec2-user python3 -u /home/ec2-user/agent/agent.py > /home/ec2-user/agent/run.log  2>&1 &
echo "agent start successfully " | tee -a $LOG_FILE

install_crontab() {
    echo "Start install crontab..." | tee -a $LOG_FILE
    sudo yum install cronie -y | tee -a $LOG_FILE
}

check_crontab() {
    if command -v crontab &> /dev/null
    then
        echo "crontab install successfully!" | tee -a $LOG_FILE
        return 0
    else
        echo "crontab install failed!" | tee -a $LOG_FILE
        return 1
    fi
}

while true; do
    install_crontab
    if check_crontab; then
        break
    else
        echo "Retry to install crontab..." | tee -a $LOG_FILE
    fi
done

echo "crontab install successfully and checked!" | tee -a $LOG_FILE

# Start crontab for agent monitor
sudo systemctl start crond
sudo systemctl enable crond
sudo -i
echo "0 * * * * sh /home/ec2-user/agent/monitor_agent.sh" > /tmp/cron_temp
crontab /tmp/cron_temp
echo "crontab add successfully " | tee -a $LOG_FILE

echo "setup script end>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"