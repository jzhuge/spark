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
Wrapper for run.py that sets up arguments for sql-runner.py.
"""

import os, sys

FLAGS_TO_REMOVE = set(['-H', '-S', '--silent', '-v', '--verbose'])

def getenv(name, default=None, required=True):
    """Return the value of an env variable.
    If the env var doesn't exist, returns default.
    If required, the env var doesn't exist, and default is None, fails.
    """
    value = os.getenv(name, default)
    if value is None and required:
        raise Exception("Environment variable not set: %s" % name)
    return value

def get_all(args, *arg_keys):
    """Returns all arguments and values for the given key.

    The return value is the matching list and a list of other args
    """
    remaining = []
    matching = []
    i = 0
    while i < len(args):
        if args[i] in arg_keys:
            matching.append(args[i])
            i += 1
            matching.append(args[i])
        else:
            remaining.append(args[i])

        i += 1

    return (matching, remaining)

def main(args):
    """Main method for calling sql-runner.py
    """
    spark_home = getenv('SPARK_HOME')

    # remove hive flags that are not supported
    args = [ arg for arg in args if arg not in FLAGS_TO_REMOVE ]

    # separate the run-sql arguments from the Spark arguments
    sql_args, spark_args = get_all(args, '--hiveconf', '--hivevar', '-d', '--define')

    # build the final command args
    command = '%s/bin/run.py' % spark_home
    command_args = [command]
    command_args.extend(spark_args)
    command_args.append('--deploy-mode')
    command_args.append('client')
    command_args.append('%s/bin/sql-runner.py' % spark_home)
    command_args.extend(sql_args)

    # run the spark-submit script
    os.execv(command, command_args)


if __name__ == '__main__':
    main(sys.argv[1:])
