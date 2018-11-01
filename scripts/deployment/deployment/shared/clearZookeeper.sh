#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.util.ResetZookeeper
ARGUMENTS="--name something"
cd ../

bash deploy_dutihr.sh $MAINCLASS $ARGUMENTS true

cd shared