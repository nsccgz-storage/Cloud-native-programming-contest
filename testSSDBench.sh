#!/bin/bash

set -x

current=`date "+%Y-%m-%d-%H-%M-%S"`

LOG_DIR=`pwd`/mylogs/SSDBench

mkdir -p ${LOG_DIR}
LOG_PATH=${LOG_DIR}/${current}.log


dbPath=/mnt/ssd/testDB

cd src/main/java
rm SSDBench.class
javac SSDBench.java
rm ${dbPath}
java SSDBench ${dbPath} 2>&1 | tee ${LOG_PATH}
