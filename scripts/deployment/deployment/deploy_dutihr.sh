#!/usr/bin/env bash
source ../../parameters.sh
echo "Working dir is `pwd`"
echo "Sources dir is $LOCAL_CODEFEEDR_SOURCES"

PWD=`pwd`

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

if [ -z "$SKIP_JAR_COPY" ]
	then
	
	cd $LOCAL_CODEFEEDR_SOURCES
	echo "Building sources in `pwd`"
	sbt assembly
	cp $LOCAL_ARTIFACT_FOLDER/root-assembly-0.1.0-SNAPSHOT.jar $LOCAL_CODEFEEDR_SOURCES/codefeedr.jar
	scp $LOCAL_CODEFEEDR_SOURCES/codefeedr.jar nvankaam@dutihr.st.ewi.tudelft.nl:~/codefeedr.jar
else
	echo "Not copying jar because third argument was passed. Assuming jar already exists"
fi

cd $PWD
ssh nvankaam@dutihr.st.ewi.tudelft.nl  'bash -s' < $LOCAL_CODEFEEDR_SOURCES/scripts/deployment/deployment/deploy_job.sh "$MAIN_CLASS $ARGUMENTS"
