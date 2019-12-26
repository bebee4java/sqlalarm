#!/bin/bash

#set -x

for env in SPARK_HOME ; do
  if [[ -z "${!env}" ]]; then
    echo "$env must be set to run this script"
    exit 1
  else
    echo ${env}=${!env}
  fi
done

if [[ -z "${SQLALARM_HOME}" ]]; then
  export SQLALARM_HOME="$(cd "`dirname "$0"`"/../; pwd)"
fi

echo "SQLALARM_HOME=$SQLALARM_HOME"

MAIN_JAR=$(find ${SQLALARM_HOME}/*/target -type f -name "*.jar" \
| grep 'sa-core' |grep -v "sources" | grep -v "original" | grep -v "javadoc")

echo "MAIN_JAR=$MAIN_JAR"

export DRIVER_MEMORY=${DRIVER_MEMORY:-2g}
${SPARK_HOME}/bin/spark-submit --class dt.sql.alarm.SQLAlarmBoot \
        --driver-memory ${DRIVER_MEMORY} \
        --master "local[*]" \
        --name SQLALARM \
        --conf "spark.driver.extraJavaOptions"="-DREALTIME_LOG_HOME=$SQLALARM_HOME/logs" \
        --conf "spark.sql.hive.thriftServer.singleSession=true" \
        --conf "spark.kryoserializer.buffer=256k" \
        --conf "spark.kryoserializer.buffer.max=1024m" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --conf "spark.scheduler.mode=FAIR" \
        ${MAIN_JAR} \
        -sqlalarm.name sqlalarm \
        -redis.addresses "127.0.0.1:6379" \
        -redis.database 4 \
        -sqlalarm.sources kafka \
        -sqlalarm.input.kafka.topic sqlalarm_event \
        -sqlalarm.input.kafka.subscribe.topic.pattern 1 \
        -sqlalarm.input.kafka.bootstrap.servers "127.0.0.1:9092" \
        -sqlalarm.sinks console
        
