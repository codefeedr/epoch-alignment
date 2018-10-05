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


#Topic info
kafka-topics.sh  --zookeeper "$ZOOKEEPER" --describe --topic codefeedr.*


# Messages in topic
TOPIC="codefeedr_HotIssue_ae327cf4-0006-47ab-abb2-a35986c87e6b"
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $TOPIC --time -1 --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'