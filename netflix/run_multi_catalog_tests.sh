#!/usr/bin/env bash

export HADOOP_CONF_DIR=${PWD}/netflix/test-conf
export _JAVA_OPTIONS="-Dspark.log.path=spark.log"

case "$OSTYPE" in
  darwin*) common_opts=("--extra-properties-file" "netflix/test-conf/spark-mac.properties") ;;
esac

opts_log=("--driver-java-options" "-Dspark.root.logger=INFO,logFile")

set -ex

bin/spark-submit --master 'local[*]' "${opts_log[@]}" "${common_opts[@]}" netflix/multi_catalog_tests.py
