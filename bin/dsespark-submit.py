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


def getenv(name, required=True):
    """Return the value of an env variable.
    If the env var doesn't exist, return None if required is False, fails otherwise."""
    value = os.getenv(name)
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


class SparkSubmitEnvironment(object):
    """Extracts the required environment variables/configs for spark-submit"""
    def __init__(self):
        self.spark_home = getenv('SPARK_HOME')
        self.spark_conf_dir = getenv('SPARK_CONF_DIR')
        self.spark_config_options = getenv('SPARK_CONFIG_OPTS', False)
        self.hadoop_home = getenv('HADOOP_HOME')
        self.hadoop_conf_dir = getenv('HADOOP_CONF_DIR')
        self.current_job_tmp_dir = getenv('CURRENT_JOB_TMP_DIR')
        self.current_job_working_dir = getenv('CURRENT_JOB_WORKING_DIR')
        self.history_server_address = self._parse_history_server_address()

    def _parse_history_server_address(self):
        """Return the history server from yarn-site.xml"""
        yarn_site_file = '%s/mapred-site.xml' % self.hadoop_conf_dir
        history_server_address = get_property_value_from_xml(yarn_site_file, 'mapreduce.jobhistory.address', False)
        if history_server_address:
            return history_server_address.split(':')[0]
        else:
            return None

    def get_genie_job_id(self):
        """Return genie job id. Supports both genie 2 and 3"""
        # check GENIE_JOB_ID first, which is defined in Genie 3
        genie_job_id = getenv('GENIE_JOB_ID', False)
        if genie_job_id:
            return genie_job_id
        else:
            return os.path.basename(self.current_job_working_dir)

    def is_spinnaker_cluster(self):
        """Returns true if the command is invoked for a Spinnaker cluster, false otherwise."""
        yarn_site_file = '%s/yarn-site.xml' % self.hadoop_conf_dir
        return get_property_value_from_xml(yarn_site_file, 'aws.jobflowid', False) is None

    def __str__(self):
        return 'SparkSubmitEnvironment = {spark_home=%s,' \
               'spark_conf_dir=%s,' \
               'spark_config_options=%s,' \
               'hadoop_home=%s,' \
               'hadoop_conf_dir=%s,' \
               'current_job_tmp_dir=%s,' \
               'current_job_working_dir=%s,' \
               'history_server_address=%s}' \
               % (self.spark_home,
                  self.spark_conf_dir,
                  self.spark_config_options,
                  self.hadoop_home,
                  self.hadoop_conf_dir,
                  self.current_job_tmp_dir,
                  self.current_job_working_dir,
                  self.history_server_address)

DEFAULT_DRIVER_MEM_MB = 5120

def get_driver_memory():
    mem_mb = DEFAULT_DRIVER_MEM_MB

    genie_mem = getenv('GENIE_JOB_MEMORY', False)
    if genie_mem:
        genie_mb = int(genie_mem)
        if genie_mb > mem_mb:
            # even though the driver is not running on Genie, use Genie's memory
            # setting to bump up driver memory, in case it was used by mistake
            sys.stderr.write("WARNING: Using Genie memory setting to increase spark.driver.memory\n")
            mem_mb = genie_mb

    return str(mem_mb) + 'm'


def main(command_args):
    """main entry point"""
    spark_env = SparkSubmitEnvironment()
    spark_submit = '%s/bin/spark-submit' % spark_env.spark_home

    # args should start with the name of the executable
    spark_submit_args = [spark_submit]

    if spark_env.spark_config_options:
        spark_submit_args.append(spark_env.spark_config_options)

    # set default command line args
    if spark_env.history_server_address:
        spark_submit_args.append('--conf')
        spark_submit_args.append('spark.yarn.historyServer.address=%s:18080'
                                 % spark_env.history_server_address)

    spark_submit_args.append('--conf')
    spark_submit_args.append('spark.genie.id=%s' % spark_env.get_genie_job_id())

    if spark_env.is_spinnaker_cluster():
        yarn_site_file = '%s/yarn-site.xml' % getenv('HADOOP_CONF_DIR')
        node_labels_enabled = get_property_value_from_xml(yarn_site_file, 'yarn.node-labels.enabled', False)
        if node_labels_enabled == 'true':
             spark_submit_args.append('--conf')
             spark_submit_args.append('spark.yarn.am.nodeLabelExpression=datanode')
             spark_submit_args.append('--conf')
             spark_submit_args.append('spark.yarn.executor.nodeLabelExpression=datanode||nodemanager')

    spark_submit_args.append('--conf')
    spark_submit_args.append('spark.s3.use.instance.credentials=true')

    spark_submit_args.append('--deploy-mode')
    spark_submit_args.append('cluster')

    spark_submit_args.append('--properties-file')
    spark_submit_args.append(os.path.join(spark_env.spark_home, 'conf', 'spark-defaults.conf'))

    # then add user's command line args
    spark_submit_args.extend(command_args)

    hadoop_classpath = commands.getoutput('%s/bin/hadoop classpath' % spark_env.hadoop_home)
    # this is overridden by spark.driver.memory
    os.environ['SPARK_DRIVER_MEMORY'] = get_driver_memory()
    os.environ['SPARK_SUBMIT_OPTS'] = '-Djava.io.tmpdir=%s -cp %s:%s/jars/*:%s' \
                                      % (spark_env.current_job_tmp_dir,
                                         spark_env.spark_conf_dir,
                                         spark_env.spark_home,
                                         hadoop_classpath)
    os.environ['MALLOC_ARENA_MAX'] = '4'
    os.execv(spark_submit, spark_submit_args)

if __name__ == "__main__":
    main(sys.argv[1:])
