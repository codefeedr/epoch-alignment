#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka
./deployHotIssueQueryKafkaSink.sh
./deployHotPullrequestKafkaSource.sh
