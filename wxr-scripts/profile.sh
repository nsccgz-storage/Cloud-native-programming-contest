#!/bin/bash

PROFILE_PATH=/home/wyf/nfs/software/envs/mqJavaClass/async-profile/async-profiler-2.5-linux-x64


current=`date "+%Y-%m-%d-%H-%M-%S"`

LOG_DIR=/home/wxr/project/Cloud-native-programming-contest/mylogs/${current}
#LOG_DIR=./mylogs/test/${current}
mkdir ${LOG_DIR}


#${PROFILE_PATH}/profiler.sh -d 5 MQBench

cd ${LOG_DIR}

${PROFILE_PATH}/profiler.sh -e cpu -d 60 -f cpu_profile.html MQBench

#python3 -m http.server