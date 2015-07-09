#!/bin/bash
# run the startup script, logging output, using the most modern 2.x python you can find
export PYTHON=`which python27`
if [ "" = "$PYTHON" ]; then export PYTHON=`which python26` ; fi
if [ "" = "$PYTHON" ]; then export PYTHON=`which python25` ; fi
if [ "" = "$PYTHON" ]; then export PYTHON=`which python` ; fi
/root/config_node_for_ica.sh 2>&1 | tee -a /var/log/eca_config.log
echo "$PYTHON /root/start_node.py /mnt/userdata/userdata.json 2>&1 >> /mnt/eca-rmpi &"  | tee -a /var/log/eca_config.log
$PYTHON /root/start_node.py /mnt/userdata/userdata.json 2>&1 >> /mnt/eca-rmpi &
pid=$!
echo $pid > /var/run/eca-rmpi.pid
disown -h $pid
