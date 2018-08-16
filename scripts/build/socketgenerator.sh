#!/bin/sh
CURRENTDIR=`pwd`

cd ../../
docker-compose build socketgenerator

cd $CURRENTDIR