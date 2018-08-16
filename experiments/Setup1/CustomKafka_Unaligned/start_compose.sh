#!/bin/bash


printenv



# File used to keep track of previous run numbers, to keep runs unique
FILE="runIncrement.txt"


if [ ! -f $FILE ]; then
   RUNINCREMENT=0
else
   RUNINCREMENT=`cat $FILE`
fi

RUNINCREMENT=$(($RUNINCREMENT+1))
echo $RUNINCREMENT > $FILE




echo "Starting with run increment: ${RUNINCREMENT}"
export RUNINCREMENT=${RUNINCREMENT}

docker-compose up

read -p "Press any key to continue... " -n1 -s