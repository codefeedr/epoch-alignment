#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.AlignmentController
ARGUMENTS="--alignmentSubject HotIssue"
cd ../

bash deploy_dutihr.sh $MAINCLASS $1 "$ARGUMENTS"