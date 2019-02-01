#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotIssueQueryKafkaSink
ARGUMENTS="--name something"
cd ../

bash deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd experiment1