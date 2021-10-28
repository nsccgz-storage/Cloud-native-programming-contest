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

LOGDIR=/home/wxr/project/mylogs
LOG_PATH=${LOGDIR}/${current}.log

DBDIR=/mnt/nvme/wxr
#DBDIR=/mnt/ssd/wxr
PMEMDIR=/mnt/pmem/wxr

ulimit -s unlimited

export LD_LIBRARY_PATH=/home/wyf/nfs/software/spack/opt/spack/linux-centos7-cascadelake/gcc-9.4.0/pmdk-1.9-v5dq7ypxskas4r7j7pyk7wsg2qxfyypj/lib:${LD_LIBRARY_PATH}
export PATH=/home/wyf/nfs/software/envs/maven/apache-maven-3.8.2/bin:${PATH}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.302.b08-0.el7_9.x86_64/jre



mkdir -p ${DBDIR}
mkdir -p ${PMEMDIR}
rm -rf  ${DBDIR}/*
rm -rf ${PMEMDIR}/*


ls -l ${DBDIR}
ls -l ${PMEMDIR}

mvn clean package -Dmaven.test.skip=true assembly:single

taskset -c 5-8 java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.MQBench  ${DBDIR} ${PMEMDIR} 0 | tee ${LOG_PATH}

ls -l ${DBDIR}
ls -l ${PMEMDIR}



ls -l ${DBDIR}
ls -l ${PMEMDIR}