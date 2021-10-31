##!/bin/bash
#
#PROFILE_PATH=/home/wyf/nfs/software/envs/mqJavaClass/async-profile/async-profiler-2.5-linux-x64
#
#
#current=`date "+%Y-%m-%d-%H-%M-%S"`
#
#LOG_DIR=/home/wxr/project/mylogs/flameGraph/${current}
##LOG_DIR=./mylogs/test/${current}
#mkdir ${LOG_DIR}
#
#cd ${LOG_DIR}
#
#${PROFILE_PATH}/profiler.sh -e cpu -d 60 -f cpu_profile.html MQBench
#
##python3 -m http.server

#e!/bin/bash

PROFILE_PATH=/home/wyf/nfs/software/envs/mqJavaClass/async-profile/async-profiler-2.5-linux-x64

set -x


current=`date "+%Y-%m-%d-%H-%M-%S"`
LOG_DIR=/home/wxr/project/mylogs/flameGraph/${current}
mkdir ${LOG_DIR}
RESULT_PATH=${LOG_DIR}/


#${PROFILE_PATH}/profiler.sh -d 5 MQBench

cd ${RESULT_PATH}

INTEVEL=10
ITER=0
RET=0

while [[ ${RET} -eq 0 ]]
do

${PROFILE_PATH}/profiler.sh -e cpu -d ${INTEVEL} -f ${ITER}.html MQBench
RET=$?
ITER=`expr ${ITER} + ${INTEVEL}`

done


#python3 -m http.server