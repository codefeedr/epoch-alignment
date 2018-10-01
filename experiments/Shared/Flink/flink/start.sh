#!/bin/bash
#Give permission on log files
chmod o+x /opt/flink/logs
chmod 777 /opt/flink/logs

pwd

sh /docker-entrypoint.sh "$1"