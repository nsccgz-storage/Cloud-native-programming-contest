#!/bin/bash


dbPath=/mnt/ssd/testDB

cd src/main/java
javac SSDBench.java
rm ${dbPath}
java SSDBench ${dbPath}