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


def getenv(name, required=True, default=None):
    """Return the value of an env variable.
    If the env var doesn't exist, return default if required is False, fails otherwise."""
    value = os.getenv(name, default)
    if value is None and required:
        raise Exception("Environment variable not set: %s" % name)
    return value


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
        self.resource_manager_address = self._parse_resource_manager_address()

    def _parse_resource_manager_address(self):
        """Return the resource manager address from yarn-site.xml"""
        yarn_site_file = '%s/yarn-site.xml' % self.hadoop_conf_dir
        tree = xml_parser.parse(yarn_site_file).getroot()
        for yarn_property in tree.findall('property'):
            name = yarn_property.find('name').text
            value = yarn_property.find('value').text  # host:port
            if name == 'yarn.resourcemanager.address' or name == 'yarn.resourcemanager.hostname':
                return value.split(':')[0]
        raise Exception('Cannot find yarn.resourcemanager.address/yarn.resourcemanager.hostname '
                        'properties in %s' % yarn_site_file)

    def get_all_jars_to_ship(self, args):
        """Return all jars to ship (user's jars + default jars shipped with all jobs)"""

        default_jars = ','.join(["s3://atlas.us-east-1.prod.netflix.net/jars/atlas-hive.jar",
                                 "%s/hive-site.xml" % self.spark_conf_dir])

        for index, arg in enumerate(args):
            try:
                if arg == '--jars':
                    user_jars = args[index + 1]
                    return '%s,%s' % (default_jars, user_jars)
            except IndexError:
                raise Exception('Missing value for --jars')
        return default_jars

    def get_hadoop_classpath(self):
        return commands.getoutput('%s/bin/hadoop classpath' % self.hadoop_home)

    def get_spark_driver_classpath(self):
        """Returns the driver class path"""
        return "%s:%s:%s" % (self.spark_conf_dir,
                                os.path.join(self.spark_home, "jars", "*"),
                                self.get_hadoop_classpath())

    def get_log_file(self):
        """Returns the log file path and also sets appropriate env variables so spark will honor the log settings."""
        spark_log_dir = getenv('SPARK_LOG_DIR', False, getenv('CURRENT_JOB_TMP_DIR'))
        os.environ['SPARK_LOG_DIR'] = spark_log_dir
        spark_log_file = getenv('SPARK_LOG_FILE', False, 'sparkshell.log')
        os.environ['SPARK_LOG_FILE'] = spark_log_file
        os.environ['SPARK_LOG_FILE_PATH'] = os.path.join(spark_log_dir, spark_log_file)
        os.environ['SPARK_PRINT_LAUNCH_COMMAND'] = 'True'
        sys.stderr.write('Spark client logs are located at %s\n' % getenv('SPARK_LOG_FILE_PATH'))
        return getenv('SPARK_LOG_FILE_PATH')

    def __str__(self):
        return 'SparkSubmitEnvironment = {spark_home=%s,' \
               'spark_conf_dir=%s,' \
               'spark_config_options=%s,' \
               'hadoop_home=%s,' \
               'hadoop_conf_dir=%s,' \
               'current_job_tmp_dir=%s,' \
               'current_job_working_dir=%s,' \
               'resource_manager_address=%s}' \
               % (self.spark_home,
                  self.spark_conf_dir,
                  self.spark_config_options,
                  self.hadoop_home,
                  self.hadoop_conf_dir,
                  self.current_job_tmp_dir,
                  self.current_job_working_dir,
                  self.resource_manager_address)


DEFAULT_DRIVER_MEM_MB = 3072

def get_driver_memory():
    mem_mb = DEFAULT_DRIVER_MEM_MB
    genie_mem = getenv('GENIE_JOB_MEMORY', False)
    if genie_mem:
        genie_mb = int(genie_mem)
        if genie_mb > mem_mb:
            mem_mb = genie_mb

    return str(mem_mb) + 'm'


def main(command_args):
    """main entry point"""
    spark_env = SparkSubmitEnvironment()
    spark_shell = '%s/bin/%s' % (spark_env.spark_home, command_args[0])


    # args should start with the name of the executable and in case of shell the first arg need to be passed first.
    spark_shell_args = [spark_shell]

    if spark_env.spark_config_options:
        spark_shell_args.append(spark_env.spark_config_options)

    # set default command line args
    spark_shell_args.append('--conf')
    spark_shell_args.append('spark.yarn.historyServer.address=%s:18080'
                             % spark_env.resource_manager_address)

    spark_shell_args.append('--conf')
    spark_shell_args.append('spark.genie.id=%s' % getenv('GENIE_JOB_ID', False))

    # spark_shell_args.append('--conf')
    # spark_shell_args.append('spark.netflix.environment=%s' % getenv('NETFLIX_ENVIRONMENT', False))
    #
    # spark_shell_args.append('--conf')
    # spark_shell_args.append('spark.netflix.stack=%s' % getenv('NETFLIX_STACK', False))

    jars = spark_env.get_all_jars_to_ship(command_args)
    if '--jars' in command_args:
        index = command_args.index('--jars')
        command_args[index + 1] = jars
    else:
        spark_shell_args.append('--jars')
        spark_shell_args.append(jars)

    spark_shell_args.append('--driver-class-path')
    spark_shell_args.append(spark_env.get_spark_driver_classpath())

    spark_shell_args.append('--driver-java-options')
    spark_shell_args.append("-Djava.io.tmpdir=%s -Dspark.log.path=%s -XX:OnOutOfMemoryError='kill -9 %%p'" % (
        spark_env.current_job_tmp_dir, spark_env.get_log_file()))

    spark_shell_args.append('--properties-file')
    spark_shell_args.append(os.path.join(spark_env.spark_home, 'conf', 'spark-defaults.conf'))

    # then add user's command line args, we already added first arg in beginning so skip it.
    spark_shell_args.extend(command_args[1:])

    hadoop_classpath = spark_env.get_hadoop_classpath()
    # this will be overridden by spark.driver.memory
    os.environ['SPARK_DRIVER_MEMORY'] = get_driver_memory()
    os.environ['SPARK_DRIVER_CLASSPATH'] = spark_env.get_spark_driver_classpath()
    os.environ['MALLOC_ARENA_MAX'] = '4'
    os.environ['SPARK_PRINT_LAUNCH_COMMAND'] = 'True'
    os.execv(spark_shell, spark_shell_args)


if __name__ == "__main__":
    main(sys.argv[1:])
