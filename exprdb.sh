#!/bin/bash -x
nohup java -server -mx20g -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport $1 &
