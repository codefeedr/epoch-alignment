#!/usr/bin/env bash

MAINCLASS=org.codefeedr.experiments.HotIssueQuery
ARGUMENTS="--name something"
cd ../

sh deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd experiment1