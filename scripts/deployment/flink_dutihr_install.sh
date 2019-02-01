#!/usr/bin/env bash
source ../parameters.sh

cd $LOCAL_CODEFEEDR_SOURCES

if [ -z "$1" ]
  then
	echo "Building dependencies"
    sbt assemblyPackageDependency
	cp $LOCAL_ARTIFACT_FOLDER/root-assembly-0.1.0-SNAPSHOT-deps.jar $LOCAL_CODEFEEDR_SOURCES/codefeedr-deps.jar
else 
	echo "Skipping dependency building because of argument $1"
fi 

# Build the dependencies jar, that is placed on the flink cluster



scp $LOCAL_CODEFEEDR_SOURCES/scripts/parameters.sh nvankaam@dutihr.st.ewi.tudelft.nl:~/parameters.sh
scp $LOCAL_CODEFEEDR_SOURCES/codefeedr-deps.jar nvankaam@dutihr.st.ewi.tudelft.nl:~/codefeedr-deps.jar

#Docker version: Docker version 17.12.0-ce, build c97c6d6

ssh nvankaam@dutihr.st.ewi.tudelft.nl 'bash -s' < $LOCAL_CODEFEEDR_SOURCES/scripts/deployment/scripts/install_flink.sh
