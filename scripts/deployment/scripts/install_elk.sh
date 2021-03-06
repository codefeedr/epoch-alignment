#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=afstuderen
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/epoch-alignment

rm -rf $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/$CODEFEEDR_SOURCES
mkdir -p $INSTALL_ROOT/codefeedr/experiments
cd $INSTALL_ROOT/$CODEFEEDR_SOURCES

git clone https://github.com/codefeedr/epoch-alignment.git
cd $REPO_FOLDER
git checkout master

#Start ELK stack
cd $REPO_FOLDER/experiments/Shared/Elk
docker-compose up --force-recreate -d

#TODO:Import XML file into kibana