#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotPullrequestStandalone
ARGUMENTS="something"
cd ../

bash deploy_dutihr.sh $MAINCLASS false $ARGUMENTS

cd experiment1