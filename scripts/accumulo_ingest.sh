#!/usr/bin/env bash
export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/etc/hadoop
spark-submit --class org.stat.spark.CSVIngestor --master local[2] /tmp/stat-1.0-SNAPSHOT.jar ingest /tmp/getstat_com_serp_report_201707.csv accumulo accumulo
