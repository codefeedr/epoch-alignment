#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=codefeedr/repo
CODEFEEDR_EXPERIMENTS=$INSTALL_ROOT/codefeedr/experiments
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/codefeedr

MAIN_CLASS=$1
if [ -z "$1" ]
  then
    echo "No main class supplied"
	exit 1
fi

ARGUMENTS=$2
if [ -z "$2" ]
  then
    echo "No arguments supplied"
	exit 1
fi


JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})

docker cp $CODEFEEDR_EXPERIMENTS/codefeedr.jar "$JOBMANAGER_CONTAINER:/codefeedr.jar"
docker exec -d "$JOBMANAGER_CONTAINER" flink run -c org.codefeedr.experiments.HotIssueQuery /codefeedr.jar