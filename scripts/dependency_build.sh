#!/usr/bin/env bash
cd ../
sbt "project flinkintegration" "set test in assembly := {}" assemblyPackageDependency
cp flinkintegration/target/scala-2.11/flinkintegration-assembly-0.1.0-SNAPSHOT.jar codefeedr.jar
cd scripts/