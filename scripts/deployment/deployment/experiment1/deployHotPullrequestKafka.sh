#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka
bash deployHotIssueQueryKafkaSink.sh false
bash deployHotPullrequestKafkaSource.sh true
