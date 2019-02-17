#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotIssueQueryKafkaSink



ARGUMENTS=$1
cd ../

bash deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd experiment1