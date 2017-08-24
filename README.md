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

## Project Structure

The project consists of the following modules:
### Core
Consists of the engine that composes Flink topologies, and links them together over kafka. Also contains in
- `Engine.Query` Query operators
- `input` For now, plugin inputs. Should be refactored.
- `Library` TODO: Refactoring
- `Library.Internal` TODO: Refactoring
### Model
Contains data objects used by the engine. This module is published to the Flink cluster along with the jobs.
### TestModel
Contains data objects used by the integration tests, that also need to be published to the Flink cluster. In the future maybe refactor this to an "Integration Test" plugin. For now the plugin architecture is not finished yet.


## Setup KAFKA & Zookeeper

### Option 1a: Use docker in VirtualBox (Windows 10 Home, OSX)
1. `docker build -t codefeedr/kafka containers/kafka/` From the root of the project, to build the image
2. `docker run -p 2181:2181 -p 9092:9092 --name kafka --rm --env ADVERTISED_HOST=192.168.99.100  --env ADVERTISED_PORT=9092 --env DELETE_TOPICS=true -d codefeedr/kafka` to run the kafka container. Replace hostname with the desired hostname. Also place this hostname in resources/reference.conf
3. `docker stop kafka` to stop and remove the container again
4. Use `docker-machine ip default` to obtain the IP of the docker machine. Place this IP in resources/reference.config for the kafka server and consumers.
5. If needed increase the memory available to the docker vm. See https://forums.docker.com/t/change-vm-specs-storage-base-on-windows-7/16584/2 for windows.
### Option 1b: Use docker with Virtualisation (Linux, Windows 10 professional, Windows 10 Ultimate)
1. `docker build -t codefeedr/kafka containers/kafka/` From the root of the project, to build the image
2. `docker network create --subnet=192.168.99.0/24 codefeednet` To create a subnet for the container to run in
3. `docker run --net codefeednet --ip 192.168.99.100 -p 2181:2181 -p 9092:9092 --name kafka --rm --env ADVERTISED_HOST=192.168.99.100 --env ADVERTISED_PORT=9092 --env DELETE_TOPICS=true -d codefeedr/kafka` to run the container with the same IP as it would by default in virtualbox
4. `docker stop kafka` to stop and remove the container again
5. When using a different hostname, modifiy the ADVERTISED_HOST in the docker run command and the host configured in resources/reference.conf accordingly
### Option 2 (Not advised): Run manually
1. Download the latest version of kafka from https://kafka.apache.org/
2. From the root folder run "bin/zookeeper-server-start.sh config/zookeeper.properties" to start apache zookeeper
3. From the root folder run "bin/kafka-server-start.sh config/server.properties" to start kafka

## Setup FLINK in docker
### Option 1a: Use docker in VirtualBox (Windows 10 Home, OSX)
1. `docker run --name flink -p 8081:8081 -p 6123:6123 --rm -d flink local` To run a local cluster in docker. See https://hub.docker.com/_/flink/ for documentation
2. `docker stop flink` to stop and remove the container again
3. Use `docker-machine ip default` to obtain the IP of the docker machine. Place this IP in resources/reference.config for the flink cluster.
### Option 1b: Use docker with Virtualisation (Linux, Windows 10 professional, Windows 10 Ultimate)
1. `docker network create --subnet=192.168.99.0/24 codefeednet` To create a subnet for the container to run in. (Skip this step if already performed for running kafka in docker)
2. `docker run --net codefeednet --ip 192.168.99.100 --name flink -p 8081:8081 -p 6123:6123 --rm flink local` To run a local cluster in docker. See https://hub.docker.com/_/flink/ for documentation
3. `docker stop flink` to stop and remove the container again