# CodeFeedr

A platform for streaming software analytics

[![Build Status](https://travis-ci.org/codefeedr/codefeedr.svg?branch=master)](https://travis-ci.org/codefeedr/codefeedr)
[![BCH compliance](https://bettercodehub.com/edge/badge/codefeedr/codefeedr?branch=master)](https://bettercodehub.com/)

## Configuring the build environment

### Command line

The project is build with SBT. To install SBT, do:

* Mac: `brew install sbt`
* Debian/Ubuntu: `apt-get install sbt`
 
Run `sbt`. Then at the SBT console:

- `compile` to compile
- `run` to run the default class
- `test` to run the tests
- `clean` to clean
- `scalafmt` to format according to project guidelines
- `scalafmt::test` to check if format according to project guidelines

### From IntelliJ

Install the latest IntelliJ. Go to the IntelliJ preferences and install the
Scala plugin. Then

1. File -> New -> Project with existing sources from within IntelliJ or "Import project" from the 
IntelliJ splash screen
2. Select the top level checkout directory for CodeFeedr.
3. On the dialog that appears, select "SBT"
4. Click Next and then Finish
5. From the "SBT Projet Data To Import", select all modules

In order to run your application from within IntelliJ, you have to select the classpath of the 
'mainRunner' module in  the run/debug configurations. Simply open 'Run -> Edit configurations...' 
and then select 'mainRunner' from the "Use  classpath of module" dropbox.

It is recommended to install the `scalafmt` plugin and turn on the 'Format on file save option' in the
IntelliJ preferences panel.

## Required external programs

* MongoDB
* Kafka

## Setup KAFKA

### Option 1: Use docker (Advised)
1. `docker build -t codefeedr/kafka containers/kafka/` From the root of the project, to build the image
2. `docker run -p 2181:2181 -p 9092:9092 --name kafka --rm --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 codefeedr/kafka` to run the container
3. `docker stop kafka` to stop and remove the container again
4. When using Docker Toolbox, use `docker-machine ip default` to obtain the IP of the docker machine. Place this IP in resources/reference.config for the kafka server and consumers.

### Option 2: Run manually
1. Download the latest version of kafka from https://kafka.apache.org/
2. From the root folder run "bin/zookeeper-server-start.sh config/zookeeper.properties" to start apache zookeeper
3. From the root folder run "bin/kafka-server-start.sh config/server.properties" to start kafka

Now the unit tests should run. For further info on runnin kafka, and running kafka on windows, see https://kafka.apache.org/quickstart
