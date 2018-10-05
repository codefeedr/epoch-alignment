#!/usr/bin/env bash

#Connect to installation machine kafka
ssh nvankaam@athens.ewi.tudelft.nl

#Select container that is running kafka
KAFKA_CONTAINER=$(docker ps --filter name=_kafka --format={{.ID}})

#Start shell inside container running kafka
docker exec -ti "$KAFKA_CONTAINER" bash

#Store zookeeper location in a variable
ZOOKEEPER="zookeeper:2181"

#Move to the location where the kafka CLI is installed
cd $KAFKA_HOME/bin



#List topics
kafka-topics.sh --list --zookeeper "$ZOOKEEPER"

#Delete all topics created by codefeedr
kafka-topics.sh --zookeeper "$ZOOKEEPER" --delete --topic codefeedr.*