#!/usr/bin/env bash
# Need to deploy the job publishing hot issues to kafka



a=0
step=0.05

while [ $a -lt 100 ]
do
	echo $a
    ( cd ../../ ; bash flink_dutihr_recreate.sh )
	#( cd ../../ ; bash kafka_gp5_install.sh )
	name="$1_$a"
	echo "Deploying with name $name"
	bash deployHotIssueQueryKafkaSink.sh $name
	s=$(echo "$step*$a+10" | bc)
	echo "Sleeping for $s"
	sleep $s
	bash deployHotPullrequestKafkaSource.sh $name
	
	sleep 120
	bash startSynchronization.sh $name
	sleep 90
	a=`expr $a + 1`
done



sleep 1

