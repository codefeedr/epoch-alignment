#!/bin/bash


printenv



# File used to keep track of previous run numbers, to keep runs unique
FILE="runIncrement.txt"


if [ ! -f $FILE ]; then
   RUNINCREMENT=0
else
   RUNINCREMENT=`cat $FILE`
fi

RUNINCREMENT=$(($RUNINCREMENT+1))
echo $RUNINCREMENT > $FILE




echo "Starting with run increment: ${RUNINCREMENT}"
export RUNINCREMENT=${RUNINCREMENT}
docker-compose up -d

# Retrieve container that is running the jobmanager
$JOBMANAGER_CONTAINER=(docker ps --filter name=jobmanager --format "{{.ID}}")

Write-Host "Submitting job to container $JOBMANAGER_CONTAINER"

# Submit the job to the flink cluster
docker cp importlinkedcache.jar "${JOBMANAGER_CONTAINER}:/jobs/socketreceiver.jar"
docker cp submitcir.sh "${JOBMANAGER_CONTAINER}:/jobs/socketreceiver.sh"
docker exec --detach "$JOBMANAGER_CONTAINER" "/socketreceiver.sh"





read -p "Press any key to continue... " -n1 -s