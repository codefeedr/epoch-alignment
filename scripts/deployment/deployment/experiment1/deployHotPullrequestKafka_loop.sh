#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka



a=0
step=0.05

while [ $a -lt 40 ]
do
	echo $a
    ( cd ../../ ; bash flink_dutihr_recreate.sh )
	( cd ../../ ; bash kafka_gp5_install.sh )
	echo "Waiting"
	sleep 5
	name="$1_$a"
	echo "Deploying with name $name"
	bash deployHotIssueQueryKafkaSink.sh $name
	s=$(echo "$step*$a+1" | bc)
	echo "Sleeping for $s"
	sleep $s
	bash deployHotPullrequestKafkaSource.sh $name
	a=`expr $a + 1`
	sleep 60
	bash startSynchronization.sh $name
	sleep 360
done



sleep 1

