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
Wrapper for spark-submit script.
"""
import sys
import os
import os.path
import commands
import xml.etree.ElementTree as xml_parser

DEFAULT_DRIVER_MEM_MB = {
        'client': 5120,
        'cluster': 3072
    }

DEFAULT_DEPLOY_MODE = {
        'spark-sql': 'client',
        'spark-sql': 'client',
        'spark-shell': 'client',
        'spark-console': 'client',
        'scala-console': 'client',
        'scala-kernel': 'client',
        'sql-console': 'client',
        'spark-submit': 'cluster',
        'pyspark': 'cluster'
    }


def getenv(name, default=None, required=True):
    """Return the value of an env variable.
    If the env var doesn't exist, returns default.
    If required, the env var doesn't exist, and default is None, fails.
    """
    value = os.getenv(name, default)
    if value is None and required:
        raise Exception("Environment variable not set: %s" % name)
    return value


def get_property_value_from_xml(xml_path, property_name, required):
    tree = xml_parser.parse(xml_path).getroot()
    for yarn_property in tree.findall('property'):
        name = yarn_property.find('name').text
        value = yarn_property.find('value').text  # host:port
        if name == property_name:
            return value
    if required:
        raise Exception('Cannot find %s properties in %s' % (property_name,xml_path))
    else:
        return None

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


def is_spinnaker_cluster(yarn_site):
    """Returns true if the command is invoked for a Spinnaker cluster, false otherwise."""
    return get_property_value_from_xml(yarn_site, 'aws.jobflowid', False) is None

def get_history_server_address(mapred_site):
    """Return the history server from mapred-site.xml"""
    history_server_address = get_property_value_from_xml(mapred_site, 'mapreduce.jobhistory.address', False)
    if history_server_address:
        return history_server_address.split(':')[0]
    else:
        return None

def get_genie_job_id(current_job_working_dir):
    """Return genie job id. Supports both genie 2 and 3"""
    # check GENIE_JOB_ID first, which is defined in Genie 3
    genie_job_id = getenv('GENIE_JOB_ID', False)
    if genie_job_id:
        return genie_job_id
    else:
        return os.path.basename(current_job_working_dir)


def get_jars(spark_conf_dir, args):
    """Return all jars to ship (user's jars + default jars shipped with all jobs)"""
    default_jars = ','.join(["s3://atlas.us-east-1.prod.netflix.net/jars/atlas-hive.jar"])
    user_jars, remaining = get_value(args, '--jars')
    if user_jars:
        return ('%s,%s' % (default_jars, user_jars), remaining)
    else:
        return (default_jars, remaining)

def get_driver_memory(mode):
    mem_mb = DEFAULT_DRIVER_MEM_MB[mode]

    genie_mem = getenv('GENIE_JOB_MEMORY', False)
    if genie_mem:
        genie_mb = int(genie_mem)
        if genie_mb > mem_mb:
            if mode == 'cluster':
                # even though the driver is not running on Genie, use Genie's memory
                # setting to bump up driver memory, in case it was used by mistake
                sys.stderr.write("WARNING: spark-submit: Using Genie memory setting to increase spark.driver.memory\n")
            mem_mb = genie_mb

    return str(mem_mb) + 'm'


def main(command_args):
    """main entry point"""

    spark_home = getenv('SPARK_HOME')
    spark_conf_dir = getenv('SPARK_CONF_DIR')
    hadoop_home = getenv('HADOOP_HOME')
    hadoop_conf_dir = getenv('HADOOP_CONF_DIR')
    mapred_site = '%s/mapred-site.xml' % hadoop_conf_dir
    yarn_site = '%s/yarn-site.xml' % hadoop_conf_dir

    current_job_tmp_dir = getenv('CURRENT_JOB_TMP_DIR')
    current_job_working_dir = getenv('CURRENT_JOB_WORKING_DIR')

    command = command_args[0]
    if command not in ['spark-submit', 'spark-shell', 'spark-sql']:
        spark_command = 'spark-submit'
    else:
        spark_command = command
    spark_executable = '%s/bin/%s' % (spark_home, spark_command)

    # args should start with the name of the executable
    spark_args = [spark_command]

    # add the Spark properties file
    spark_args.append('--properties-file')
    spark_args.append(os.path.join(spark_home, 'conf', 'spark-defaults.conf'))

    # add cluster-specific properties if the file exists
    spark_cluster_properties = os.path.join(hadoop_conf_dir, 'spark-cluster.properties')
    if os.path.exists(spark_cluster_properties):
        spark_args.append('--extra-properties-file')
        spark_args.append(spark_cluster_properties)

    # add configuration from SPARK_CONFIG_OPTS, after Spark and cluster defaults.
    spark_config_options = getenv('SPARK_CONFIG_OPTS', required=False)
    if spark_config_options:
        spark_args.extend(spark_config_options.split(' '))

    # add the Genie ID
    spark_args.append('--conf')
    spark_args.append('spark.genie.id=%s' % get_genie_job_id(current_job_working_dir))

    # add a default log path
    spark_args.append('--conf')
    spark_args.append('spark.log.path=spark.log')

    # DEPRECATED: remove this when spark-cluster.properties contains these values.
    # set default command line args
    history_server_address = get_history_server_address(mapred_site)
    if history_server_address:
        spark_args.append('--conf')
        spark_args.append('spark.yarn.historyServer.address=%s:18080' % history_server_address)

    # DEPRECATED
    if is_spinnaker_cluster(yarn_site):
        node_labels_enabled = get_property_value_from_xml(yarn_site, 'yarn.node-labels.enabled', False)
        if node_labels_enabled == 'true':
             spark_args.append('--conf')
             spark_args.append('spark.yarn.am.nodeLabelExpression=datanode')
             spark_args.append('--conf')
             spark_args.append('spark.yarn.executor.nodeLabelExpression=datanode||nodemanager')

    # add Jars
    jars, command_args = get_jars(spark_conf_dir, command_args)
    spark_args.append('--jars')
    spark_args.append(jars)

    # set deploy mode
    deploy_mode, command_args = get_value(command_args, '--deploy-mode')
    if not deploy_mode:
        deploy_mode = getenv('DEPLOY_MODE', required=False)
    if not deploy_mode and command in DEFAULT_DEPLOY_MODE:
        deploy_mode = DEFAULT_DEPLOY_MODE[command]
    if not deploy_mode:
        deploy_mode = 'client'
    spark_args.append('--deploy-mode')
    spark_args.append(deploy_mode)

    # last, add user's command line args
    spark_args.extend(command_args[1:])

    # set the temp folder for all JVMs
    java_options = [getenv('_JAVA_OPTIONS', required=False)]
    if None in java_options:
        java_options.remove(None)
    java_options.append('-Djava.io.tmpdir=%s' % current_job_tmp_dir)
    os.environ['_JAVA_OPTIONS'] = ' '.join(java_options)

    # this is overridden by spark.driver.memory
    os.environ['SPARK_DRIVER_MEMORY'] = get_driver_memory(deploy_mode)
    os.environ['MALLOC_ARENA_MAX'] = '4'
    if getenv('DEBUG', default='false', required=False).lower() == 'true':
        os.environ['SPARK_PRINT_LAUNCH_COMMAND'] = 'True'
        sys.stderr.write("Execv: %s %s\n" % (spark_executable, repr(spark_args)))

    os.execv(spark_executable, spark_args)

if __name__ == "__main__":
    main(sys.argv[1:])
