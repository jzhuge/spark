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
import shutil
import commands
import xml.etree.ElementTree as xml_parser
import glob


DEFAULT_DRIVER_MEM_MB = {
        'client': 5120,
        'cluster': 5120
    }

DEFAULT_DEPLOY_MODE = {
        'spark-sql': 'client',
        'spark-sql': 'client',
        'spark-shell': 'client',
        'spark-console': 'client',
        'scala-console': 'client',
        'scala-kernel': 'client',
        'sparklyr': 'client',
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

def write_config_yml(spark_args):
    # yaml is available on the big data image, but not Genie
    # Genie doesn't run R kernels, so this only imports yaml when it is needed
    import yaml

    # sparklyr does not use the default master from spark-defaults.conf
    configs = {
            'spark.master': 'yarn'
        }
    i = 1 # skip spark command
    while i < len(spark_args):
        if spark_args[i] == '--conf':
            i += 1
            key, value = spark_args[i].split('=', 1)
            configs[key] = value

        elif spark_args[i].startswith('--'):
            key = 'sparklyr.shell.' + spark_args[i].lstrip('--')
            if not spark_args[i+1].startswith('--'):
                i += 1
                configs[key] = spark_args[i]
            else:
                configs[key] = ''

        i += 1

    with open('config.yml', 'w') as config_yml:
        yaml.dump(
                { 'default': configs },
                config_yml,
                default_flow_style=False
            )

def clean_delimiter_args(command_args):
    '''Extract spark_args and kernel_args for pyspark-kernel commands
    '''
    split_point = command_args.index('DELIMITER')
    spark_args = command_args[:split_point]
    kernel_args = command_args[split_point+1:]
    return (spark_args, kernel_args)

def get_venv_path(profile):
    ''' Gets the latest published version of the big data image python environment built

    :param profile: one of python2 or python3
    :return:
    '''
    venv_path = 'hdfs:///venv/{}/bdp-python-meta/current.tar.gz'.format(profile)

    sys.stderr.write("Using Python virtual environment: {} ".format(venv_path))

    return venv_path


def main(command_args):
    """main entry point"""

    spark_home = getenv('SPARK_HOME')
    spark_conf_dir = getenv('SPARK_CONF_DIR')
    hadoop_home = getenv('HADOOP_HOME')
    hadoop_conf_dir = getenv('HADOOP_CONF_DIR')
    mapred_site = '%s/mapred-site.xml' % hadoop_conf_dir
    yarn_site = '%s/yarn-site.xml' % hadoop_conf_dir

    # copy hive-site.xml into jars if it is present.
    # adding this to Jars ensures that it is in the driver's classpath.
    hive_site = '%s/hive-site.xml' % hadoop_conf_dir
    if os.path.exists(hive_site):
        shutil.copy(hive_site, '%s/jars/hive-site.xml' % spark_home)

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

    if command == 'pyspark-kernel-unstable':
        (cmd_spark_args, cmd_kernel_args) = clean_delimiter_args(command_args)
        spark_python = os.path.join(spark_home, 'python')
        py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
        os.environ['PYTHONPATH'] = os.pathsep.join([spark_python, py4j])
        command_args = cmd_spark_args

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

    # get the python environment
    venv_profile, command_args = get_value(command_args, '--venv')
    if venv_profile:
        venv_python = './__venv__/bin/python'

        spark_args.append('--conf')
        spark_args.append('spark.yarn.python.venv={}'.format(get_venv_path(venv_profile)))
        spark_args.append('--conf')
        spark_args.append('spark.executorEnv.PYSPARK_PYTHON={}'.format(venv_python))
        spark_args.append('--conf')
        spark_args.append('spark.yarn.appMasterEnv.PYSPARK_PYTHON={}'.format(venv_python))
        # Allow for extra time before AM timeout due to unpacking tar
        spark_args.append('--conf')
        spark_args.append('spark.yarn.am.waitTime=60s')
        # Need to set this env so that in client mode the right python exec is passed through
        os.environ['PYSPARK_PYTHON'] = venv_python

    # get the command args file
    command_args_file, command_args = get_value(command_args, '--command-arg-file')

    # set the temp folder for all JVMs
    java_options = [getenv('_JAVA_OPTIONS', required=False)]
    if None in java_options:
        java_options.remove(None)
    java_options.append('-Djava.io.tmpdir=%s' % current_job_tmp_dir)
    java_options.append('-Dspark.log.path=spark.log')
    os.environ['_JAVA_OPTIONS'] = ' '.join(java_options)

    # this is overridden by spark.driver.memory
    os.environ['SPARK_DRIVER_MEMORY'] = get_driver_memory(deploy_mode)
    os.environ['MALLOC_ARENA_MAX'] = '4'
    if getenv('DEBUG', default='false', required=False).lower() == 'true':
        os.environ['SPARK_PRINT_LAUNCH_COMMAND'] = 'True'
        sys.stderr.write("Execv: %s %s\n" % (spark_executable, repr(spark_args)))

    if command == 'sparklyr' or command == 'sparklyr-kernel':
        # spark args are passed via configuration file instead of direct
        write_config_yml(spark_args)

        # command_args contains: ['sparklyr',executable,args...]
        os.execv(command_args[1], command_args[1:])
    elif command == 'pyspark-kernel-unstable':
        spark_args = spark_args[1:]
        current_job_working_dir = os.getenv('CURRENT_JOB_WORKING_DIR')
        if current_job_working_dir:
            if os.getenv('TITUS_TASK_ID'):
                spark_log_path = '/logs/{GENIE_JOB_ID}/spark.log'.format(**os.environ)
            else:
                spark_log_path = '{}.log'.format(current_job_working_dir.rstrip('/'))
            os.environ['SPARK_LOG_FILE_PATH'] = spark_log_path
            spark_args.append('--conf')
            spark_args.append("spark.log.path=" + spark_log_path)
            # add the user's spark properties, if present. this comes before spark CLI
            #  arguments so that properties set on the command line take precedence.
            extra_properties_path = os.path.expanduser('~/.spark.properties')
            if os.path.exists(extra_properties_path):
                spark_args.insert(0, '--extra-properties-file')
                spark_args.insert(1, extra_properties_path)
            else:
                extra_properties_path = os.path.expanduser('~/notebooks/spark.properties')
                if os.path.exists(extra_properties_path):
                    spark_args.insert(0, '--extra-properties-file')
                    spark_args.insert(1, extra_properties_path)
            extra_args = " ".join(['"%s"' % (x) for x in spark_args])
            os.environ['PYSPARK_SUBMIT_ARGS'] = '--verbose ' + extra_args + ' pyspark-shell'
            os.execv(cmd_kernel_args[0], cmd_kernel_args)

    elif command == 'spark-session':
        # if this is a spark-session job, append properties it needs to run
        spark_args.extend(command_args[1:])
        spark_args.append('--deploy-mode')
        spark_args.append('client')
        spark_args.append('--conf')
        spark_args.append('spark.scheduler.mode=FAIR')
        spark_args.append('--conf')
        spark_args.append('spark.scheduler.defaultpool.mode=fair')
        spark_args.append('--conf')
        spark_args.append('spark.dynamicAllocation.interactive=true')
        spark_args.append('--conf')
        spark_args.append('spark.mode.queryservice=true')
        spark_args.append('--jars')
        spark_args.append(os.environ['QUERY_SERVICE_LITE_JAR'])
        spark_args.append('--class')
        spark_args.append('com.netflix.queryservice.SparkQueryServer')
        spark_args.append(os.environ['QUERY_SERVICE_LITE_JAR'])
        os.execv(spark_executable, spark_args)
    else:
        # last, add user's command line args
        spark_args.extend(command_args[1:])

        # add command-line args for the app from the command args file
        if command_args_file and os.path.exists(command_args_file):
            with open(command_args_file) as arg_file:
                spark_args.extend([ arg[:-1] if arg.endswith('\n') else arg for arg in arg_file.readlines() ])

        os.execv(spark_executable, spark_args)

if __name__ == "__main__":
    main(sys.argv[1:])
