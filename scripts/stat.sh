#!/usr/bin/env bash

export JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk.x86_64"
export SPARK_HOME="/opt/spark-2.2.0-bin-hadoop2.7"
export HADOOP_CONF_DIR="/etc/impala/conf.dist"

/opt/spark-2.2.0-bin-hadoop2.7/bin/spark-shell --driver-memory 4G --executor-memory 4G --jars /tmp/stat-1.0-SNAPSHOT.jar
