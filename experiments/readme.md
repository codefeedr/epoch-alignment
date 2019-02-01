# Experiments

This folder contains docker configuration for the experiments to be performed


## Preparation for all experiments

```
cd Elk
docker-compose up -d
cd ../kafka-docker
docker-compose up -d
cd ../Flink
# Force recreate to dispose of any running jobs
docker-compose up --force-recreate -d
```

## UI of tooling

- Kibana: http://localdocker:5601/app/kibana
- Flink: http://localdocker:8081/#/overview

## Experiment 1

```
cd Setup1/CustomKafka_Unaligned


```