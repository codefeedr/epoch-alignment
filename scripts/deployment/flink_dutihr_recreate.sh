#!/usr/bin/env bash
PWD=`pwd`
source ../parameters.sh

scp $LOCAL_CODEFEEDR_SOURCES/scripts/parameters.sh nvankaam@dutihr.st.ewi.tudelft.nl:~/parameters.sh

ssh nvankaam@dutihr.st.ewi.tudelft.nl  'bash -s' < $LOCAL_CODEFEEDR_SOURCES/scripts/deployment/scripts/recreate_flink.sh