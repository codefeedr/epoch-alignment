#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.HotPullrequestStandalone
ARGUMENTS="--name something"
cd ../

bash deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd experiment1