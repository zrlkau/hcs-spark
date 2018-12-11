#!/bin/bash
BASEDIR=$(dirname $0)
JARS=$(readlink -f $BASEDIR/../jars)

EXTERNAL_SHUFFLE_SERVICE=$(grep "^[^#]*spark.shuffle.service.enable" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | awk '{print $2}')
NEXEC=$(grep "^[^#]*spark.scheduler.hcs.numExecutors" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | awk '{print $2}')
[ "$NEXEC" == "" ] && NEXEC=8

OPTIONS=e:
LONGOPTIONS=num-executors:

PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTIONS --name "$0" -- "$@")
eval set -- "$PARSED"
while true; do
    case "$1" in
	-e|--num-executors)
	    NEXEC="$2"
	    shift 2
	    ;;
	--)
	    shift
	    break
	    ;;
	*)
	    echo "error: $1"
	    exit 3
	    ;;
    esac
done



if [ "$EXTERNAL_SHUFFLE_SERVICE" == "true" ]; then
    for NODE in $(cat $HADOOP_HOME/etc/hadoop/slaves); do
	ssh -t $NODE $SPARK_HOME/sbin/start-shuffle-service.sh &
    done
    wait
fi

while [ $NEXEC -gt 0 ]; do 
    for NODE in $(cat $HADOOP_HOME/etc/hadoop/slaves); do
	ssh -t $NODE $SPARK_HOME/sbin/start-worker.sh &
	NEXEC=$[$NEXEC-1]
    done

    wait
done
