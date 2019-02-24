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


SKIP_JAR_COPY=$2

if [ "$SKIP_JAR_COPY" = "false" ]
	then
	cd $LOCAL_CODEFEEDR_SOURCES
	echo "Building sources in `pwd`"
	sbt assembly
	cp $LOCAL_ARTIFACT_FOLDER/root-assembly-0.1.0-SNAPSHOT.jar $LOCAL_CODEFEEDR_SOURCES/codefeedr.jar
	scp $LOCAL_CODEFEEDR_SOURCES/codefeedr.jar nvankaam@dutihr.st.ewi.tudelft.nl:~/codefeedr.jar
else
	echo "Not copying jar because third argument \"$2\" was passed. Assuming jar already exists"
fi

ARGUMENTS="$3"
if [ -z "$3" ]
  then
    echo "No name supplied"
	exit 1
else
	echo "Starting job with run name $3"
fi


echo "Connecting to remote. Arguments are $ARGUMENTS"

ssh nvankaam@dutihr.st.ewi.tudelft.nl MAIN_CLASS="$MAIN_CLASS" ARGUMENTS=\"$ARGUMENTS\" 'bash -s' <<-'ENDSSH'
	if [ -z "$MAIN_CLASS" ]
	  then
		echo "No main class supplied"
		exit 1
	  else 
		echo "Supplied main class is \"$MAIN_CLASS\"" 
	fi

	
	if [ -z "$ARGUMENTS" ]
	  then
		echo "No run name supplied"
		exit 1
	fi

	#Build new version of codefeedr

	JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})


	echo "deploying to container $JOBMANAGER_CONTAINER. Copying jar"

	docker cp ~/codefeedr.jar "$JOBMANAGER_CONTAINER:/codefeedr.jar"

	echo "Jar copied, starting job"
	echo "Arguments: $ARGUMENTS"
	#Change -d to -t when the job does not start to view eventual deployment errors
	
	if [ "$MAIN_CLASS" == "org.codefeedr.experiments.AlignmentController" ]
	then
		docker exec -t "$JOBMANAGER_CONTAINER" flink run -c $MAIN_CLASS /codefeedr.jar --run $ARGUMENTS
	else
		docker exec -d "$JOBMANAGER_CONTAINER" flink run -c $MAIN_CLASS /codefeedr.jar --run $ARGUMENTS
	fi
ENDSSH