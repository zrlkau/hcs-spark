#!/bin/bash
BASEDIR=$(dirname $0)
JARS=$(readlink -f $BASEDIR/../jars)
EXTERNAL_SHUFFLE_SERVICE=$(grep "^[^#]*spark.shuffle.service.enable" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | awk '{print $2}')

for NODE in $(cat $HADOOP_HOME/etc/hadoop/slaves); do
    ssh -t $NODE $SPARK_HOME/sbin/stop-worker.sh &
done

wait

if [ "$EXTERNAL_SHUFFLE_SERVICE" == "true" ]; then
    for NODE in $(cat $HADOOP_HOME/etc/hadoop/slaves); do
	ssh -t $NODE $SPARK_HOME/sbin/stop-shuffle-service.sh &
    done
    wait
fi
