#!/bin/bash

set -x

current=`date "+%Y-%m-%d-%H-%M-%S"`


dbPath=/mnt/nvme/testDB


/usr/bin/env /usr/lib/jvm/java-11-openjdk-11.0.12.0.7-0.el7_9.x86_64/bin/java -Dfile.encoding=UTF-8 @/tmp/cp_9zzf273crprs1fknagj5ly7kh.argfile io.openmessaging.SSDBench ${dbPath}

#LOG_DIR=`pwd`/mylogs/SSDBench

#mkdir -p ${LOG_DIR}
#LOG_PATH=${LOG_DIR}/${current}.log



#cd src/main/java
#rm SSDBench.class
#javac SSDBench.java
#rm ${dbPath}
#java SSDBench ${dbPath} 2>&1 | tee ${LOG_PATH}
