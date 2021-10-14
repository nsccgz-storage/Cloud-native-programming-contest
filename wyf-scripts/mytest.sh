#!/bin/bash


# init maven settings.xml first

# [wyf@cn3 mqJavaClass]$ cat ~/.m2/settings.xml 
# <settings>
#   <proxies>
#    <proxy>
#       <id>example-proxy</id>
#       <active>true</active>
#       <protocol>http</protocol>
#       <host>localhost</host>
#       <port>52341</port>
#     </proxy>
#   </proxies>
# </settings>

set -x

current=`date "+%Y-%m-%d-%H-%M-%S"`
LOG_DIR=./mylogs/test
mkdir ${LOG_DIR}
LOG_PATH=${LOG_DIR}/${current}.log

DBDIR=/mnt/nvme/mq
#DBDIR=/mnt/ssd/mq 
PMDIR=/mnt/pmem/mq

ulimit -s unlimited

export PATH=/home/wyf/nfs/software/envs/maven/apache-maven-3.8.2/bin:${PATH}

# dirname $(dirname $(readlink -f $(which javac)))
# java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' 


# on cn3
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.292.b10-1.el7_9.x86_64/jre
# on cn6
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.275.b01-0.el7_9.x86_64/jre/
#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.302.b08-0.el7_9.x86_64

export JAVA_HOME=/usr/java/jdk1.8.0_301-amd64



mkdir -p ${DBDIR}
mkdir -p ${PMDIR}


rm -rf  ${DBDIR}/*
rm -rf  ${PMDIR}/*

ls -l ${DBDIR}

#mvn clean package assembly:single test
mvn clean package assembly:single -DskipTests


# mvn exec:java -Dexec.mainClass="io.openmessaging.Test1" -Dexec.args="${DBDIR}" -Dexec.classpathScope=test  -e

rm -rf  ${DBDIR}/*

taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx20g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.MQBench  ${DBDIR} ${PMDIR} 2>&1 | tee ${LOG_PATH}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx170g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.MQBench  ${DBDIR} ${PMDIR} | tee ${LOG_PATH}

#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx170g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.PMPrefetchBuffer  ${DBDIR} ${PMDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.WYFTest  ${DBDIR} ${PMDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.PMBench  ${DBDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.Test1  ${DBDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.SSDBench  ${DBDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.Test1Benchmark  ${DBDIR}
#java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.Test1Pmem  ${DBDIR}
#taskset -c 1-4 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.UnsafeUtil  ${DBDIR}

ls -l ${DBDIR}
