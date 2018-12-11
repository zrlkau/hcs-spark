#!/bin/bash

BASEDIR=$(dirname $0)
LOGDIR=$(readlink -f $BASEDIR/../logs)
JARS=$(readlink -f $BASEDIR/../jars)
export CLASSPATH=$JARS/*:$CLASSPATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CRAIL_HOME/lib

NCORES=1
EXEC=$(jps | grep CoarseGrainedExecutorBackend | wc -l)
LOGFILE=worker-$(hostname -f)-${EXEC}.log
HCS_URL=$(grep "^[^#]*spark.scheduler.hcs.url" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | awk '{print $2}')
JAVA_OPTS=$(grep "^[^#]*spark.executor.extraJavaOptions" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | sed 's/.*"\(.*\)"/\1/');
JAVA_EXTRA_CLASS_PATH=$(grep "^[^#]*spark.executor.extraClassPath" $SPARK_HOME/conf/spark-defaults.conf 2>/dev/null | tail -n1 | awk '{print $2}');
#JAVA_OPTS="-Xmx512m -Dsun.nio.PageAlignDirectMemory=true -Dspark.ui.port=4044"
echo "spark.executor.extraJavaOptions=$JAVA_OPTS"
echo "spark.executor.extraClassPath=$JAVA_EXTRA_CLASS_PATH"
echo "spark.scheduler.hcs.url=$HCS_URL"

export CLASSPATH=$JAVA_EXTRA_CLASS_PATH:$CLASSPATH

nohup $JAVA_HOME/bin/java -server $JAVA_OPTS \
      org.apache.spark.executor.CoarseGrainedExecutorBackend --hcs-master $HCS_URL --hostname $(hostname -f) --cores ${NCORES} &> ${LOGDIR}/${LOGFILE} &
PID=$!

echo "Started worker $EXEC on $(hostname -f)"
echo " - process id = $PID"
echo " - logfile    = ${LOGDIR}/${LOGFILE}"

sleep 1

if ! jps | grep "^$PID " &>/dev/null; then
    echo " - status     = Failed. Check log file."
else
    echo " - status     = Running"
fi


