#!/usr/bin/env bash
source ~/parameters.sh

cd $TARGET_CODEFEEDR_SOURCES/experiments/Shared/Flink
docker-compose down

cd ~

rm -rf $TARGET_CODEFEEDR_SOURCES
mkdir -p $TARGET_INSTALL_ROOT
cd $TARGET_INSTALL_ROOT

git clone https://github.com/codefeedr/epoch-alignment.git
cd $TARGET_CODEFEEDR_SOURCES
git checkout epochalignment



cd $TARGET_CODEFEEDR_SOURCES/experiments/Shared/Flink
cp ~/codefeedr-deps.jar flink/lib/codefeedr-deps.jar 

#Start docker services
docker-compose build
docker-compose up -d --force-recreate 
docker-compose scale taskmanager=2

#TODO:Import XML file into kibana