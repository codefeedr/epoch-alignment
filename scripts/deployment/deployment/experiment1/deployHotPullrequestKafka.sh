#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka
./deployHotIssueQueryKafkaSink.sh

MAINCLASS=org.codefeedr.experiments.HotPullrequestKafkaSource
ARGUMENTS="--name something"
cd ../

bash deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd experiment1