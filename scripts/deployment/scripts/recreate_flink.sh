#!/usr/bin/env bash
source ~/parameters.sh

cd $TARGET_CODEFEEDR_SOURCES
git pull

cd $TARGET_CODEFEEDR_SOURCES/experiments/Shared/Flink
#Start docker services
docker-compose up -d --force-recreate 
#docker-compose scale taskmanager=6 

#TODO:Import XML file into kibana