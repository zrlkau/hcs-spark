#!/bin/bash

BASEDIR=$(dirname $0)
LOGDIR=$(readlink -f $BASEDIR/../logs)
JARS=$(readlink -f $BASEDIR/../jars)
export CLASSPATH=$JARS/*:$CLASSPATH

NCORES=1
EXEC=$(jps | grep CoarseGrainedExecutorBackend | wc -l)

TRAIL=0
while [ $EXEC -gt 0 ]; do
    for PID in $(jps | grep CoarseGrainedExecutorBackend | awk '{print $1}'); do
	echo "Stopping executor ${PID} on $(hostname)..."
	[ $TRAIL -lt 10 ] && kill $PID || kill -9 $PID
    done
    sleep 1
    TRAIL=$[$TRAIL+1]
    EXEC=$(jps | grep CoarseGrainedExecutorBackend | wc -l)
done
