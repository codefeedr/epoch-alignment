#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.AlignmentController
ARGUMENTS="--alignmentSubject HotIssue --alignmentSource HotIssueSource_82c6916b-f984-4586-b77a-bf898667dcba"
cd ../

bash deploy_dutihr.sh $MAINCLASS $1 "$ARGUMENTS"