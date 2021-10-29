#!/bin/bash
#timeout 20 stress --vm 1  --vm-bytes 172G --vm-keep
taskset -c 9 stress --vm 1  --vm-bytes 148G --vm-keep
