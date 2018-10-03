#!/usr/bin/env bash

INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=afstuderen
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/codefeedr


cd $REPO_FOLDER
git pull

cd $REPO_FOLDER/experiments/Shared/Flink
#Start docker services
docker-compose up -d --force-recreate 
docker-compose scale taskmanager=6 

#TODO:Import XML file into kibana