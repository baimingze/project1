#!/bin/bash

#  Copyright (C) 2010 Insilicos LLC  All Rights Reserved
#  Original authors: Jeff Howbert, Natalie Tasman, Brian Pratt

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# call as
#  sshtest.sh <IP address to test> <id for log>
#
# will create /var/log/eca-sshtest-<id>.log

ssh_dest_ip=$1
log_id=$2
logfile=/var/log/eca-sshtest-$log_id.log

date=`date`
echo "testing ssh to client $log_id, ip address $ssh_dest_ip" > $logfile
echo $date >> $logfile
echo >> $logfile

status=1

while [ $status -ne 0 ]; do
ssh -vvv -F /etc/ssh/ssh_config -l root -i /root/.ssh/id_rsa "root@$ssh_dest_ip" "echo success" >> $logfile 2>&1
status=$?
echo "test completed with status $status" >> $logfile
done
echo "ssh test succeeded for root@$ssh_dest_ip" >>  $logfile
echo >> $logfile
# enable ssh to root within cluster if this is ubuntu        
if [ -d "/home/ubuntu" ];  then
  ssh ubuntu@$ssh_dest_ip 'sudo cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/' 
fi
exit $status
