#!/bin/bash

#DBDIR=/mnt/nvme/mq
DBDIR=/mnt/ssd/mq

ulimit -s unlimited

export PATH=/home/wyf/nfs/software/envs/maven/apache-maven-3.8.2/bin:${PATH}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.292.b10-1.el7_9.x86_64/jre

mkdir -p ${DBDIR}

rm -rf  ${DBDIR}/*

ls -l ${DBDIR}

#java  io.openmessaging.Test1 
#-cp "/home/wyf/nfs/0code/nsccgz-storage/Cloud-native-programming-contest/target/classes:/home/wyf/.m2/repository/org/slf4j/slf4j-log4j12/1.7.6/slf4j-log4j12-1.7.6.jar:/home/wyf/.m2/repository/org/slf4j/slf4j-api/1.7.6/slf4j-api-1.7.6.jar:/home/wyf/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar:/home/wyf/.m2/repository/com/intel/pmem/llpl/1.2.0-release/llpl-1.2.0

#/usr/bin/env /usr/lib/jvm/java-11-openjdk-11.0.12.0.7-0.el7_9.x86_64/bin/java -Dfile.encoding=UTF-8 @/tmp/cp_9zzf273crprs1fknagj5ly7kh.argfile -Xmx32g  -Xss1g io.openmessaging.Test1

#/usr/bin/env /usr/lib/jvm/java-11-openjdk-11.0.12.0.7-0.el7_9.x86_64/bin/java -Dfile.encoding=UTF-8 @/tmp/cp_9zzf273crprs1fknagj5ly7kh.argfile -Xmx128g  -Xss2g io.openmessaging.TestSSDqueue 

# on cn3
/usr/bin/env /usr/lib/jvm/java-11/bin/java -Dfile.encoding=UTF-8 @/tmp/cp_9zzf273crprs1fknagj5ly7kh.argfile -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.Test1  ${DBDIR}

ls -l ${DBDIR}
