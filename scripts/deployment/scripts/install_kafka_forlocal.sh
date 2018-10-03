#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=afstuderen_forlocal
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/codefeedr

rm -rf $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/codefeedr/experiments
cd $INSTALL_ROOT/$CODEFEEDR_SOURCES

git clone https://github.com/codefeedr/codefeedr.git
cd $REPO_FOLDER
git checkout epochalignment
cd $REPO_FOLDER/experiments/Shared/kafka-docker

#Start docker services
docker-compose -f docker-compose-zookeeper.yml up --force-recreate -d

#TODO:Import XML file into kibana