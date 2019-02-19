#!/usr/bin/env python3

import random
import textwrap

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# spark-related setup

_IS_SPARK = True

_SPARK_ERROR_MESSAGES = {
    'missing-column': 'not enough data columns',
    'extra-column': 'too many data columns',
    'cannot-cast': 'Cannot safely cast',
    'cannot-delete': 'Cannot delete file where some, but not all, rows match filter',
    'already-exists': 'already exists'
}


class _LazySparkSession(object):
    def __init__(self):
        self._spark = None

    @property
    def get(self):
        if not self._spark:
            print("Waiting for a Spark session to start...")
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def stop(self):
        if self._spark:
            print("Stop the Spark session")
            self._spark.stop()

    def __getattr__(self, name):
        return getattr(self.get, name)

    def __dir__(self):
        return dir(self.get)

    def __str__(self):
        return self.get.__str__()

    def __repr__(self):
        return self.get.__repr__()


spark = _LazySparkSession()


# helper methods

# decorator for spark-only tests, usually for dataframe functionality
def spark_only(method):
    if _IS_SPARK:
        return method
    else:
        def empty_test(self):
            pass
        return empty_test


def temp_table_name(base_name, db='bdp_integration_tests', catalog=None, unique=True):
    if unique:
        table = '{}_{}'.format(base_name, random.randint(0, 65535))
    else:
        table = base_name

    if catalog:
        return '{}.{}.{}'.format(catalog, db, table)
    else:
        return '{}.{}'.format(db, table)


class temp_table:
    def __init__(self, base_name, sql=None, *args):
        self.table_name = temp_table_name(base_name)
        self.sql = sql
        self.args = args

    def __enter__(self):
        sql("DROP TABLE IF EXISTS {0}".format(self.table_name))
        if self.sql:
            sql(self.sql, self.table_name, *self.args)
        return self.table_name

    def __exit__(self, etype, evalue, traceback):
        sql("DROP TABLE IF EXISTS {0}".format(self.table_name))
        return False # don't suppress exceptions


class temp_vault_table(temp_table):
    def __init__(self, base_name, sql=None, *args):
        self.table_name = temp_table_name('bdp_itests_' + base_name, db='vault')
        self.sql = sql
        self.args = args


def sort_by(col_name, rows):
    return sorted(rows, cmp = lambda a, b: cmp(a[col_name], b[col_name]))


def sort_by_id(rows):
    return sort_by('id', rows)


def collect(df):
    return list(map(lambda r: r.asDict(), df.collect()))


def sql(command, *args):
    query = command.format(*args) if args else command
    print(textwrap.dedent(query))
    return spark.sql(query)


def table(table_name):
    print('Query table ' + table_name)
    return spark.table(table_name)


def jvm_error(command, *args):
    try:
        sql(command, *args)
        return None
    except Exception as e:
        return str(e.java_exception)


def analysis_error(case, command, *args):
    try:
        sql(command, *args)
        return None
    except Exception as e:
        case.assertIsInstance(e, AnalysisException)
        return e.desc # return the error message


def expected_error_text(desc):
    if _IS_SPARK and desc in _SPARK_ERROR_MESSAGES:
        return _SPARK_ERROR_MESSAGES[desc]
    else:
        raise Exception("Could not find error message: " + str(desc))


def schema(table):
    return [(row['col_name'], row['data_type']) for row in collect(sql("DESCRIBE {0}", table))]


def schema_with_comments(table):
    return [(row['col_name'], row['data_type'], row['comment']) for row in collect(sql("DESCRIBE {0}", table))]
