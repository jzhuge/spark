#!/usr/bin/env python

import os, sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s',
        filename='sql-runner.log')
LOG = logging.getLogger('sql-runner')

FLAGS_TO_REMOVE = set(['-H', '-S', '--silent', '-v', '--verbose'])

def get_value(args, arg_key, default_value=None):
    """Returns an argument's last value from the list of args

    The return value is the value and the list of other args
    """
    remaining = []
    value = default_value
    i = 0
    while i < len(args):
        if arg_key == args[i]:
            i += 1
            value = args[i]
        else:
            remaining.append(args[i])

        i += 1

    return (value, remaining)

def get_values(args, arg_key):
    """Returns all values for an argument from the list of args

    The return value is the values list and a list of other args
    """
    remaining = []
    values = []
    i = 0
    while i < len(args):
        if arg_key == args[i]:
            i += 1
            values.append(args[i])
        else:
            remaining.append(args[i])

        i += 1

    return (values, remaining)

def read_code_block(code_file):
    if code_file:
        with open(code_file) as f:
            return f.read()
    return None

def remove_comments(sql):
    if sql:
        return "\n".join([ arg.split("--")[0] for arg in sql.split("\n") ])
    return None

def parse_statements(sql):
    if sql:
        return [ statement.strip() for statement in remove_comments(sql).split(";") ]
    return []


def main(args):
    """Main method for batch SQL jobs.

    This mimics the batch behavior and command-line options of Hive.

    The following Hive options are supported:
		 -d,--define <key=value>          Variable subsitution to apply to hive
										  commands. e.g. -d A=B or --define A=B
			--database <databasename>     Specify the database to use
		 -e <quoted-query-string>         SQL from command line
		 -f <filename>                    SQL from files
			--hiveconf <property=value>   Use value for given property
			--hivevar <key=value>         Variable subsitution to apply to hive
										  commands. e.g. --hivevar A=B
		 -i <filename>                    Initialization SQL file

	The remaining Hive options are not implemented, but are not passed to Spark:
		 -H                               Print help information
		 -S,--silent                      Silent mode in interactive shell
		 -v,--verbose                     Verbose mode (echo executed SQL to the
										  console)
    """

    print_header = True

    # configure the SparkSession builder
    session_builder = SparkSession.builder.appName('SparkSQL Runner')

    # translate --hiveconf, --hivevar, and -d/--define args
    hiveconfs, args = get_values(args, '--hiveconf')
    for hiveconf in hiveconfs:
        (property, value) = hiveconf.split("=")
        if property == 'hive.cli.print.header':
            print_header = value.lower() == 'true'
        else:
            session_builder.config('spark.hadoop.' + property, value)
            session_builder.config('spark.session.' + property, value)

    hivevars, args = get_values(args, '--hivevar')
    for hivevar in hivevars:
        (varname, value) = hivevar.split("=")
        session_builder.config('spark.session.' + varname, value)

    definitions, args = get_values(args, '-d')
    for definition in definitions:
        (varname, value) = definition.split("=")
        session_builder.config('spark.session.' + varname, value)

    definitions, args = get_values(args, '--define')
    for definition in definitions:
        (varname, value) = definition.split("=")
        session_builder.config('spark.session.' + varname, value)

    # get code that will be run in notebook cells
    setup, args = get_value(args, '-i')
    query, args = get_value(args, '-e')
    script, args = get_value(args, '-f')

    # remove hive flags that shouldn't be passed to Spark
    args = [ arg for arg in args if arg not in FLAGS_TO_REMOVE ]

    # fail if there are any unknown arguments
    if args:
        raise Exception("Unknown arguments: " + ", ".join(args))

    # get the sequence of statements to run
    statements = []
    statements.extend(parse_statements(read_code_block(setup)))
    statements.extend(parse_statements(query))
    statements.extend(parse_statements(read_code_block(script)))

    # remove empty statements
    statements = [ statement for statement in statements if statement ]

    # exit early if there is no work to do
    if len(statements) < 1:
        return 0

    # start the configured session
    spark = session_builder.getOrCreate()

    intermediate_statements = statements[:-1]
    output_statement = statements[-1]

    for statement in intermediate_statements:
        # run the statement and print the result to stderr using pandas
        LOG.info("Running intermediate SQL: " + statement)
        pdf = spark.sql(statement).toPandas()
        LOG.info("Result:\n" + pdf.to_string(header=print_header, index=False))

    # run the final statement and write its result as TSV to stdout
    LOG.info("Running output SQL: " + statement)
    output_pdf = spark.sql(output_statement).toPandas()
    output_pdf.to_csv(sys.stdout, header=print_header, index=False)


if __name__ == '__main__':
    main(sys.argv[1:])
