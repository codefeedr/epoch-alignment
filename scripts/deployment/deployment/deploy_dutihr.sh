#!/usr/bin/env bash
INSTALL_ROOT=/home/nvankaam
CODEFEEDR_SOURCES=codefeedr/repo
CODEFEEDR_EXPERIMENTS=$INSTALL_ROOT/codefeedr/experiments
REPO_FOLDER=$INSTALL_ROOT/$CODEFEEDR_SOURCES/codefeedr
SOURCE_FOLDER=/mnt/c/Users/nvank/Documents/Afstuderen/codefeedr

CURRENT_WORKDIR=`pwd`

MAIN_CLASS=$1
if [ -z "$1" ]
  then
    echo "No main class supplied"
	exit 1
else 
	echo "Using main class $MAIN_CLASS"
fi 




ARGUMENTS=$2
if [ -z "$2" ]
  then
    echo "No arguments supplied"
	exit 1
fi

#SKIP_JAR_COPY=$3


echo "current working dir $CURRENT_WORKDIR"


if [ -z "$SKIP_JAR_COPY" ]
	then
scp $SOURCE_FOLDER/codefeedr.jar nvankaam@dutihr.st.ewi.tudelft.nl:$CODEFEEDR_EXPERIMENTS/codefeedr.jar
else
	echo "Not copying jar because third argument was passed. Assuming jar already exists"
fi

ssh nvankaam@dutihr.st.ewi.tudelft.nl  'bash -s' < deploy_job.sh "$MAIN_CLASS $ARGUMENTS"
