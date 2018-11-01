#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotPullrequestKafkaSource
ARGUMENTS="something"
cd ../



bash deploy_dutihr.sh $MAINCLASS $1 $ARGUMENTS

cd experiment1