#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.AlignmentController
ARGUMENTS="--alignmentSubject HotIssue --alignmentSource HotIssueSource_c14d8ea7-a1cc-4700-a288-17e9c06e58d8"
cd ../

bash deploy_dutihr.sh $MAINCLASS true "$ARGUMENTS"