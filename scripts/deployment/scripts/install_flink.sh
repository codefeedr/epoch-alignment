#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=codefeedr/repo
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/codefeedr

rm -rf $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/$CODEFEEDR_SOURCES
cd $INSTALL_ROOT/$CODEFEEDR_SOURCES

git clone https://github.com/codefeedr/codefeedr.git
cd $REPO_FOLDER
git checkout epochalignment


cd $REPO_FOLDER/experiments/Shared/Flink
#Start docker services
docker-compose up -d --force-recreate 
docker-compose scale taskmanager=6 

#TODO:Import XML file into kibana