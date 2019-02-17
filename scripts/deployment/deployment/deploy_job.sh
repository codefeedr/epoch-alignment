#!/usr/bin/env bash
source ~/parameters.sh

MAIN_CLASS=$1
if [ -z "$1" ]
  then
    echo "No main class supplied"
	exit 1
  else 
	echo "Supplied main class is \"$MAIN_CLASS\"" 
fi

NAME=$2
if [ -z "$2" ]
  then
    echo "No name supplied"
	exit 1
else
	echo "Executing with run \"$NAME\""
fi

#Build new version of codefeedr

JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})


echo "deploying to container $JOBMANAGER_CONTAINER. Copying jar"

docker cp ~/codefeedr.jar "$JOBMANAGER_CONTAINER:/codefeedr.jar"

echo "Jar copied, starting job"
#Change -d to -t when the job does not start to view eventual deployment errors
docker exec -t "$JOBMANAGER_CONTAINER" flink run -c $MAIN_CLASS /codefeedr.jar --run $NAME