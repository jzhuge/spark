#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Spark History server run script
"""

import os, sys
import commands
import tempfile
import urllib, urlparse

# java -cp "$SPARK_HOME/jars/*:`$HADOOP_HOME/bin/hadoop classpath`" -Xmx9000m -Dspark.history.fs.max.size=14530242360 -Dspark.history.fs.cleaner.maxAge=365d -Dspark.history.ui.port=0 org.apache.spark.deploy.history.HistoryServer --dir history

# exec setuidgid hadoop /apps/bdp-java/java-8-oracle/bin/java -cp /apps/spark/jars/*:${HADOOP_CLASSPATH} \
#         -Dspark.history.fs.logDirectory=hdfs:///spark-logs -XX:+UseG1GC -XX:HeapDumpPath=/var/log/spark/spark-historyserver.hprof \
#         -Dspark.history.fs.max.size=10g \
#         -XX:+HeapDumpOnOutOfMemoryError -verbose:gc -Xloggc:/var/log/spark/spark-historyserver-gc.log -XX:+PrintGCDetails \
#         -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:OnOutOfMemoryError="kill -9 %p" -XX:+UseGCLogFileRotation \
#         -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9015 \
#         -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false \
#         -Dcom.sun.management.jmxremote.ssl=false -XX:NewRatio=1 -XX:+UseStringDeduplication -Xmx32g org.apache.spark.deploy.history.HistoryServer

def getenv(name, default=None, required=True):
    """Return the value of an env variable.
    If the env var doesn't exist, returns default.
    If required, the env var doesn't exist, and default is None, fails.
    """
    value = os.getenv(name, default)
    if value is None and required:
        raise Exception("Environment variable not set: %s" % name)
    return value


def get_value(args, arg_key):
    """Returns an argument's value from the list of args

    The return value is the value and the list of other args
    """
    remaining = []
    value = None
    i = 0
    while i < len(args):
        if arg_key == args[i]:
            i += 1
            value = args[i]
        else:
            remaining.append(args[i])

        i += 1

    return (value, remaining)


def path2url(path):
    return urlparse.urljoin('file:', urllib.pathname2url(os.path.abspath(path)))


def main(args):
    """Main entry point
    """
    spark_home = getenv('SPARK_HOME')
    hadoop_conf_dir = getenv('HADOOP_CONF_DIR')
    hadoop_home = getenv('HADOOP_HOME')
    java_home = getenv('JAVA_HOME')

    # the original command defaulted to using spark-history in the local directory
    history_dir = path2url('spark-history')
    if len(args) == 1 and not args[0].startswith('-'):
        file_or_dir = args[0]
        if file_or_dir.startswith('hdfs:'):
            # download the file from HDFS
            temp_dir = tempfile.mkdtemp()
            commands.getoutput(
                    '%s/bin/hadoop fs -copyToLocal %s* %s' % (
                            hadoop_home, file_or_dir, temp_dir
                        )
                )
            history_dir = path2url(temp_dir)

        elif file_or_dir.startswith('application_'):
            # download the file from HDFS
            temp_dir = tempfile.mkdtemp()
            commands.getoutput(
                    '%s/bin/hadoop fs -copyToLocal hdfs:/spark-logs/%s* %s' % (
                            hadoop_home, file_or_dir, temp_dir
                        )
                )
            history_dir = path2url(temp_dir)

        else:
            history_dir = file_or_dir

    hadoop_classpath = commands.getoutput("%s/bin/hadoop classpath" % (hadoop_home,))
    classpath = "%s/jars/*:%s" % (spark_home, hadoop_classpath)

    command = "%s/bin/java" % (java_home,)
    command_args = ['', # this is removed by java for some reason
            '-cp', classpath,
            '-Xmx9000m',
            '-Dspark.history.fs.max.size=20g',
            '-Dspark.history.fs.cleaner.maxAge=365d',
            '-Dspark.history.ui.port=0',
            '-Dspark.ui.retainedDeadExecutors=10000',
            'org.apache.spark.deploy.history.HistoryServer',
            history_dir
        ]

    if getenv('DEBUG', required=False):
        sys.stderr.write("%s %s\n" % (command, repr(command_args)))
    os.execv(command, command_args)


if __name__ == "__main__":
    main(sys.argv[2:])
