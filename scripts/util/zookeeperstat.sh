#!/usr/bin/env bash

echo stat | nc athens.ewi.tudelft.nl 2181
echo cons | nc athens.ewi.tudelft.nl 2181

echo stat | nc localhost 2181


#Select container that is running kafka
ZK_CONTAINER=$(docker ps --filter name=_zookeeper --format={{.ID}})

#Find volumes of a container
docker inspect "$ZK_CONTAINER" | grep volumes