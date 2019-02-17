#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotPullrequestKafkaSource
ARGUMENTS=$1
cd ../



bash deploy_dutihr.sh $MAINCLASS $2 "$ARGUMENTS"

cd experiment1