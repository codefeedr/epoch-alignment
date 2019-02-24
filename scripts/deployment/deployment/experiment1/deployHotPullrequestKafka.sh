#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka


bash ../shared/clearZookeeper.sh

bash deployHotIssueQueryKafkaSink.sh $1
sleep 1
bash deployHotPullrequestKafkaSource.sh $1
