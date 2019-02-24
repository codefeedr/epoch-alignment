#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka


#bash ../shared/clearZookeeper.sh

( cd ../../ ; bash flink_dutihr_recreate.sh )
( cd ../../ ; bash kafka_gp5_install.sh )

bash deployHotIssueQueryKafkaSink.sh $1
sleep 2
bash deployHotPullrequestKafkaSource.sh $1
