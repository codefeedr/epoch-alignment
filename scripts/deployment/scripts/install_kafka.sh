#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=afstuderen
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/epoch-alignment

cd $REPO_FOLDER/experiments/Shared/kafka-docker
docker-compose down -v

rm -rf $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/codefeedr/experiments
cd $INSTALL_ROOT/$CODEFEEDR_SOURCES

git clone https://github.com/codefeedr/epoch-alignment.git
cd $REPO_FOLDER
git checkout master
cd $REPO_FOLDER/experiments/Shared/kafka-docker


# Force remove kafka and zookeeper
docker rm --force kafkadocker_kafka_1
docker rm --force kafkadocker_zookeeper_1

#Start docker services
docker-compose -f docker-compose-experiment.yml up -d --build
