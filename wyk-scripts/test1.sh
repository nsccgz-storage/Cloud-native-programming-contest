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

DBDIR=/mnt/nvme/wyk
#DBDIR=/mnt/ssd/wyk
PMEMDIR=/mnt/pmem/wyk

ulimit -s unlimited

export LD_LIBRARY_PATH=/home/wyf/nfs/software/spack/opt/spack/linux-centos7-cascadelake/gcc-9.4.0/pmdk-1.9-v5dq7ypxskas4r7j7pyk7wsg2qxfyypj/lib:${LD_LIBRARY_PATH}
export PATH=/home/wyf/nfs/software/envs/maven/apache-maven-3.8.2/bin:${PATH}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.292.b10-1.el7_9.x86_64/jre


mkdir -p ${DBDIR}
mkdir -p ${PMEMDIR}
rm -rf  ${DBDIR}/*
rm -rf ${PMEMDIR}/*


ls -l ${DBDIR}
ls -l ${PMEMDIR}

mvn clean package -Dmaven.test.skip=true assembly:single

# mvn exec:java -Dexec.mainClass="io.openmessaging.Test1" -Dexec.args="${DBDIR}" -Dexec.classpathScope=test  -e

# taskset -c 1-4 java -agentpath:/home/wyk/performanceJava/async-profiler-2.5-linux-x64/build/libasyncProfiler.so=start,event=cpu,file=profile.html -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.MQBench  ${DBDIR}
taskset -c 1-4 java  -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g -XX:MaxHeapSize=4g io.openmessaging.MQBench  ${DBDIR} | tee ./mylogs.txt

ls -l ${DBDIR}
ls -l ${PMEMDIR}
