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

#DBDIR=/mnt/nvme/mq
<<<<<<< HEAD
DBDIR=/mnt/ssd/wyk
=======
DBDIR=/mnt/ssd/wyk2
>>>>>>> 3dfdd41d85a5332d4388a97c5e0982e753cdbe4f

ulimit -s unlimited

export PATH=/home/wyf/nfs/software/envs/maven/apache-maven-3.8.2/bin:${PATH}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.292.b10-1.el7_9.x86_64/jre


mkdir -p ${DBDIR}


rm -rf  ${DBDIR}/*

ls -l ${DBDIR}

mvn clean package assembly:single

# mvn exec:java -Dexec.mainClass="io.openmessaging.Test1" -Dexec.args="${DBDIR}" -Dexec.classpathScope=test  -e
java -Dfile.encoding=UTF-8 -cp "./target/mq-sample.jar:/home/wyf/nfs/software/envs/mqJavaClass/log4j-1.2.17.jar:/home/wyf/nfs/software/envs/mqJavaClass/llpl-1.2.0-release.jar" -Xmx128g  -Xss1g -XX:MaxDirectMemorySize=2g io.openmessaging.TestSSDqueue  ${DBDIR}

ls -l ${DBDIR}
