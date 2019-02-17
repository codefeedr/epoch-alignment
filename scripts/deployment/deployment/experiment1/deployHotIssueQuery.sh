#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotIssueQuery
ARGUMENTS=$1
cd ../

bash deploy_dutihr.sh $MAINCLASS false $ARGUMENTS

cd experiment1