#/bin/bash


set -eu

#This file is based on: https://github.com/matrix-org/dendrite

# The mirror to download kafka from is picked from the list of mirrors at
# https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz
# Check the signature since we are downloading over HTTP.
MIRROR=http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz

# Only download the kafka if it isn't already downloaded.
test -f kafka.tgz || wget $MIRROR -O kafka.tgz
# Unpack the kafka over the top of any existing installation
mkdir -p kafka && tar xzf kafka.tgz -C kafka --strip-components 1
# Start the zookeeper running in the background.
# By default the zookeeper listens on localhost:2181
kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
# Enable topic deletion so that the integration tests can create a fresh topic
# for each test run.
echo "delete.topic.enable=true" >> kafka/config/server.properties
# Start the kafka server running in the background.
# By default the kafka listens on localhost:9092
kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties