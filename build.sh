#!/usr/bin/env bash

sbt "project flinkintegration" assembly
cp flinkintegration/target/scala-2.11/flinkintegration-assembly-0.1.0-SNAPSHOT.jar codefeedr.jar