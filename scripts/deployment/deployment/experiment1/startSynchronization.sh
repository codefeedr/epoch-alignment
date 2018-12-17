#!/usr/bin/env bash
MAINCLASS=org.codefeedr.experiments.AlignmentController
ARGUMENTS="--alignmentSubject HotIssue --alignmentSource HotIssueSource_7557ba5c-45d0-4ae3-a65e-59192c349241"
cd ../

bash deploy_dutihr.sh $MAINCLASS $1 "$ARGUMENTS"