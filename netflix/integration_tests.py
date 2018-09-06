#!/usr/bin/env python3

import sys
import traceback
import unittest
import random
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# spark-related setup

IS_SPARK = True

SPARK_ERROR_MESSAGES = {
        'missing-column': 'not enough data columns',
        'extra-column': 'too many data columns',
        'cannot-cast': 'Cannot safely cast',
        'cannot-delete': 'Cannot delete file where some, but not all, rows match filter'
    }


class Wrapper(object):
    def __init__(self, get_func):
        self.get_func = get_func
        self.accessed = False

    def get(self):
        self.accessed = True
        return self.get_func()

    def __getattr__(self, name):
        return getattr(self.get(), name)

    def __dir__(self):
        return dir(self.get())

    def __str__(self):
        return self.get().__str__()

    def __repr__(self):
        return self.get().__repr__()

_spark_accessed = False
def get_spark():
    global _spark_accessed
    if not _spark_accessed:
        _spark_accessed = True
        print("Waiting for a Spark session to start...")
    return SparkSession.builder.master('local[*]').getOrCreate()

spark = Wrapper(get_spark)

# helper methods

# decorator for spark-only tests, usually for dataframe functionality
def spark_only(method):
    if IS_SPARK:
        return method
    else:
        def empty_test(self):
            pass
        return empty_test

def temp_table_name(base_name, db='bdp_integration_tests', unique=True):
    if unique:
        return '{0}.{1}_{2}'.format(db, base_name, random.randint(0, 65535))
    else:
        return '{0}.{1}'.format(db, base_name, random.randint(0, 65535))

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

def sort_by_id(rows):
    return sorted(rows, cmp = lambda a, b: cmp(a['id'], b['id']))

def collect(df):
    return list(map(lambda r: r.asDict(), df.collect()))

def sql(command, *args):
    if args:
        return spark.sql(command.format(*args))
    else:
        return spark.sql(command)

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
    if IS_SPARK and desc in SPARK_ERROR_MESSAGES:
        return SPARK_ERROR_MESSAGES[desc]
    else:
        raise StandardError("Could not find error message: " + str(desc))

def schema(table):
    return [ (row['col_name'], row['data_type']) for row in collect(sql("DESCRIBE {0}", table)) ]

# test cases

class IcebergDDLTest(unittest.TestCase):

    def test_alter_table_add_columns(self):
        with temp_table("test_add_columns") as t:
            sql("CREATE TABLE {0} (id bigint, data string) USING iceberg", t)
            self.assertEqual(schema(t), [
                    ('id', 'bigint'),
                    ('data', 'string')
                ])

            sql("ALTER TABLE {0} ADD COLUMNS (ts timestamp)", t)
            sql("REFRESH TABLE {0}", t)
            self.assertEqual(schema(t), [
                    ('id', 'bigint'),
                    ('data', 'string'),
                    ('ts', 'timestamp')
                ])

#    def test_alter_table_add_nested_columns(self):
#        with temp_table("test_add_nested_columns") as t:
#            sql("CREATE TABLE {0} (id bigint, point struct<x: bigint,y: bigint>) USING iceberg", t)
#            self.assertEqual(schema(t), [
#                    ('id', 'bigint'),
#                    ('point', 'struct<x:bigint,y:bigint>')
#                ])
#
#            sql("ALTER TABLE {0} ADD COLUMNS (point.z bigint)", t)
#            self.assertEqual(schema(t), [
#                    ('id', 'bigint'),
#                    ('point', 'struct<x:bigint,y:bigint,z:bigint>')
#                ])


class IcebergTypesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # recreate the table each run, but keep it around as a test for others
        cls.shared_table = temp_table_name('iceberg_types', db = 'iceberg', unique = False)
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("""
            CREATE TABLE IF NOT EXISTS {0} (
                b boolean,
                i int,
                l bigint,
                f float,
                d double,
                day date,
                ts timestamp,
                s string,
                bin binary,
                d2 decimal(9, 2),
                d4 decimal(18, 4)
            ) USING iceberg
            """, cls.shared_table)
        sql("""
            INSERT INTO {0} VALUES (
                false,
                cast(1 as int),
                40000000000,
                cast(34.12 as float),
                cast(12.34 as double),
                cast('2017-12-01' as date),
                cast('2017-12-01T10:12:55.038194-08:00' as timestamp),
                'data string',
                unbase64('U3BhcmsgU1FM'),
                cast(3.14 as decimal(9, 2)),
                cast(3.1416 as decimal(18,4)))
            """, cls.shared_table)

    def test_boolean_col(self):
        rows = collect(sql('select b as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual(False, rows[0]['value'])

    def test_integer_col(self):
        rows = collect(sql('select i as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual(1, rows[0]['value'])

    def test_long_col(self):
        rows = collect(sql('select l as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual(40000000000, rows[0]['value'])

    def test_float_col(self):
        rows = collect(sql('select f as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertAlmostEqual(34.12, rows[0]['value'], 5)

    def test_long_col(self):
        rows = collect(sql('select d as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertAlmostEqual(12.34, rows[0]['value'], 12)

    def test_date_col(self):
        rows = collect(sql('select cast(day as string) as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('2017-12-01', rows[0]['value'])

    def test_timestamp_col(self):
        rows = collect(sql('select cast(ts as string) as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('2017-12-01 18:12:55.038194', rows[0]['value'])

    def test_string_col(self):
        rows = collect(sql('select s as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('data string', rows[0]['value'])

    def test_binary_col(self):
        rows = collect(sql("select decode(bin, 'UTF-8') as value from {0}", self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('Spark SQL', rows[0]['value'])

    def test_decimal_2_col(self):
        rows = collect(sql('select cast(d2 as string) as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('3.14', rows[0]['value'])

    def test_decimal_4_col(self):
        rows = collect(sql('select cast(d4 as string) as value from {0}', self.shared_table))
        self.assertEqual(1, len(rows), "Should produce one row")
        self.assertEqual('3.1416', rows[0]['value'])


class UnpartitionedIcebergTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create a test table
        cls.shared_table = temp_table_name('iceberg_unpartitioned')
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("CREATE TABLE {0} (id bigint, data string) USING iceberg", cls.shared_table)
        sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", cls.shared_table)

    @classmethod
    def tearDownClass(cls):
        # clean up the test table
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)

    @spark_only
    def test_iceberg_table_created(self):
        describe = sql("DESCRIBE FORMATTED {0}", self.shared_table)

        # describe produces a table with col_name, table_type, and comment columns, with properties mixed in after the schema
        types = list(filter(lambda r: r['col_name'].strip() == 'table_type', collect(describe)))
        self.assertEqual(len(types), 1, "Should produce one table_type property entry")
        self.assertEqual(types[0]['data_type'].strip().lower(), 'iceberg', "Should be an iceberg table")

    def test_simple_read(self):
        rows = collect(sql("select * from {0}", self.shared_table))
        self.assertEqual(rows, [
                {'id': 1, 'data': 'a'},
                {'id': 2, 'data': 'b'},
                {'id': 3, 'data': 'c'}
            ])

    def test_insert_extra_column(self):
        expected = expected_error_text('extra-column')
        err = analysis_error(self, "INSERT INTO {0} VALUES (4, 'd', 'extra')", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

    def test_insert_missing_column(self):
        expected = expected_error_text('missing-column')
        err = analysis_error(self, "INSERT INTO {0} VALUES (4)", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

    def test_insert_incompatible_type_column(self):
        expected = expected_error_text('cannot-cast')
        err = analysis_error(self, "INSERT INTO {0} VALUES ('4', 'd')", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

    def test_create_table_as_select(self):
        with temp_table(
                "ctas_test",
                "CREATE TABLE {0} USING iceberg AS SELECT * FROM {1}",
                self.shared_table) as t:
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

    def test_delete_from_rejects_bad_filter(self):
        with temp_table(
                "delete_from_test",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:
            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)
            rows = collect(sql("SELECT * FROM {0}", t))
            # verify the data was written
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

            expected = expected_error_text('cannot-delete')
            err = jvm_error("DELETE FROM {0} WHERE id < 2", t)
            self.assertIsNotNone(err, 'Should result in a JVM error: ' + expected)
            self.assertIn(expected, err)

    def test_delete_from(self):
        with temp_table(
                "delete_from_test",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:
            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)
            rows = collect(sql("SELECT * FROM {0}", t))
            # verify the data was written
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

            sql("DELETE FROM {0} WHERE id < 4", t)

            # verify the data was deleted
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(rows, [])

    def test_insert_is_append(self):
        with temp_table(
                "insert_test",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:
            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a'},
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'},
                    {'id': 3, 'data': 'c'}
                ])

    @spark_only
    def test_dataframe_read(self):
        rows = collect(spark.table(self.shared_table))
        self.assertEqual(rows, [
                {'id': 1, 'data': 'a'},
                {'id': 2, 'data': 'b'},
                {'id': 3, 'data': 'c'}
            ])

    @spark_only
    def test_dataframe_insert_by_name(self):
        with temp_table(
                "df_insert_test",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:
            spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ("data", "id")).write.byName().insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

    @spark_only
    def test_dataframe_insert(self):
        with temp_table(
                "df_insert_test",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:
            spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ("col1", "col2")).write.insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])


class IdentityPartitionedIcebergTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create a test table
        cls.shared_table = temp_table_name('iceberg_partitioned')
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)", cls.shared_table)

        cls.data_table = temp_table_name('temp_data')
        sql("CREATE TABLE {0} (id bigint, data string) STORED AS parquet", cls.data_table)
        sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", cls.data_table)
        sql("""
            INSERT INTO {0}
            SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
            FROM {1}
            """, cls.shared_table, cls.data_table)

    @classmethod
    def tearDownClass(cls):
        # clean up the test tables
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("DROP TABLE IF EXISTS {0}", cls.data_table)

    @spark_only
    def test_iceberg_table_created(self):
        describe = sql("DESCRIBE FORMATTED {0}", self.shared_table)

        # describe produces a table with col_name, table_type, and comment columns, with properties mixed in after the schema
        types = list(filter(lambda r: r['col_name'].strip() == 'table_type', collect(describe)))
        self.assertEqual(len(types), 1, "Should produce one table_type property entry")
        self.assertEqual(types[0]['data_type'].strip().lower(), 'iceberg', "Should be an iceberg table")

    def test_simple_read(self):
        rows = collect(sql("select * from {0}", self.shared_table))
        self.assertEqual(sort_by_id(rows), [
                {'id': 1, 'data': 'a', 'part': 'odd'},
                {'id': 2, 'data': 'b', 'part': 'even'},
                {'id': 3, 'data': 'c', 'part': 'odd'}
            ])

    def test_insert_extra_column(self):
        expected = expected_error_text('extra-column')
        err = analysis_error(self, "INSERT INTO {0} VALUES (4, 'd', 'even', 'extra')", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

    def test_insert_missing_column(self):
        expected = expected_error_text('missing-column')
        err = analysis_error(self, "INSERT INTO {0} VALUES (4, 'd')", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

    def test_insert_incompatible_type_column(self):
        expected = expected_error_text('cannot-cast')
        err = analysis_error(self, "INSERT INTO {0} VALUES ('4', 'd', 'even')", self.shared_table)
        self.assertIsNotNone(err, 'Should result in an analysis error: ' + expected)
        self.assertIn(expected, err)

#    def test_create_table_as_select(self):
#        with temp_table(
#                "ctas_test",
#                "CREATE TABLE {0} USING iceberg PARTITIONED BY (part) AS SELECT * FROM {1}",
#                self.shared_table) as t:
#            rows = collect(sql("SELECT * FROM {0}", t))
#            self.assertEqual(sort_by_id(rows), [
#                    {'id': 1, 'data': 'a', 'part': 'odd'},
#                    {'id': 2, 'data': 'b', 'part': 'even'},
#                    {'id': 3, 'data': 'c', 'part': 'odd'}
#                ])

    def test_delete_from_rejects_bad_filter(self):
        with temp_table(
                "delete_from_test",
                "CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)") as t:
            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM {1}
                """, t, self.data_table)

            # verify the data was written
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

            expected = expected_error_text('cannot-delete')
            err = jvm_error("DELETE FROM {0} WHERE id < 2", t)
            self.assertIsNotNone(err, 'Should result in a JVM error: ' + expected)
            self.assertIn(expected, err)

    def test_delete_from(self):
        with temp_table(
                "delete_from_test",
                "CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)") as t:
            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM {1}
                """, t, self.data_table)

            # verify the data was written
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

            sql("DELETE FROM {0} WHERE id < 3 and part = 'even'", t)

            # verify the data was deleted
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

    def test_insert_is_append(self):
        with temp_table(
                "insert_test",
                "CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)") as t:
            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM {1}
                """, t, self.data_table)

            # verify the data was written
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM {1}
                """, t, self.data_table)
            rows = collect(sql("SELECT * FROM {0}", t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

    @spark_only
    def test_dataframe_read(self):
        rows = collect(spark.table(self.shared_table))
        self.assertEqual(sort_by_id(rows), [
                {'id': 1, 'data': 'a', 'part': 'odd'},
                {'id': 2, 'data': 'b', 'part': 'even'},
                {'id': 3, 'data': 'c', 'part': 'odd'}
            ])

    @spark_only
    def test_dataframe_insert_by_name(self):
        with temp_table(
                "df_insert_test",
                "CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)") as t:
            spark.createDataFrame([("a", "odd", 1), ("b", "even", 2), ("c", "odd", 3)], ("data", "part", "id")).write.byName().insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

    @spark_only
    def test_dataframe_insert(self):
        with temp_table(
                "df_insert_test",
                "CREATE TABLE {0} (id bigint, data string, part string) USING iceberg PARTITIONED BY (part)") as t:
            spark.createDataFrame([(1, "a", "odd"), (2, "b", "even"), (3, "c", "odd")], ("col1", "col2", "col3")).write.insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])


class ParquetPartitionedBatchPatternIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create a test table
        cls.shared_table = temp_table_name('parquet_partitioned')
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("CREATE TABLE {0} (id bigint, data string) PARTITIONED BY (part string) STORED AS parquet", cls.shared_table)

        cls.data_table = temp_table_name('temp_data')
        sql("CREATE TABLE {0} (id bigint, data string) STORED AS parquet", cls.data_table)
        sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", cls.data_table)
        sql("""
            INSERT INTO {0}
            SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
            FROM {1}
            """, cls.shared_table, cls.data_table)

    @classmethod
    def tearDownClass(cls):
        # clean up the test tables
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("DROP TABLE IF EXISTS {0}", cls.data_table)

    def test_simple_read(self):
        rows = collect(sql("select * from {0}", self.shared_table))
        self.assertEqual(sort_by_id(rows), [
                {'id': 1, 'data': 'a', 'part': 'odd'},
                {'id': 2, 'data': 'b', 'part': 'even'},
                {'id': 3, 'data': 'c', 'part': 'odd'}
            ])

    def test_parquet_table_created(self):
        # describe produces a table with col_name, table_type, and comment columns, with properties mixed in after the schema
        describe = sql("DESCRIBE FORMATTED {0}", self.shared_table)

        types = list(filter(lambda r: r['col_name'].strip() == 'table_type', collect(describe)))
        self.assertEqual(len(types), 0, "Should produce no table_type property entry")

        formats = list(filter(lambda r: r['col_name'].strip() == 'InputFormat:', collect(describe)))
        self.assertEqual(len(formats), 1, "Should produce an InputFormat: property entry")
        self.assertEqual(
                formats[0]['data_type'].strip(),
                'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                "Should be a parquet table")

    @spark_only
    def test_batch_pattern_overwrite(self):
        with temp_table(
                "partitioned_batch_overwrite_test",
                "CREATE TABLE {0} (id bigint, data string) PARTITIONED BY (part string) STORED AS parquet") as t:

            # write some data as the first batch
            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM {1}
                """, t, self.data_table)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'b', 'part': 'even'},
                    {'id': 3, 'data': 'c', 'part': 'odd'}
                ])

            # second insert should overwrite all evens -- all ids * 2
            sql("""
                INSERT INTO {0}
                SELECT id, data, case when (id % 2) == 0 then 'even' else 'odd' end
                FROM (SELECT id * 2 as id, data FROM {1})
                """, t, self.data_table)
            rows = collect(spark.table(t))
            # original id:2 row was replaced by the new 'even' data
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'a', 'part': 'odd'},
                    {'id': 2, 'data': 'a', 'part': 'even'}, # data is not 'b' because it was replaced
                    {'id': 3, 'data': 'c', 'part': 'odd'},
                    {'id': 4, 'data': 'b', 'part': 'even'},
                    {'id': 6, 'data': 'c', 'part': 'even'}
                ])

            # data frame insert should also overwrite
            spark.createDataFrame([(1, 'd', 'odd'), (3, 'e', 'odd'), (5, 'f', 'odd')]).write.insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'd', 'part': 'odd'}, # note new data
                    {'id': 2, 'data': 'a', 'part': 'even'},
                    {'id': 3, 'data': 'e', 'part': 'odd'}, # note new data
                    {'id': 4, 'data': 'b', 'part': 'even'},
                    {'id': 5, 'data': 'f', 'part': 'odd'}, # note new data
                    {'id': 6, 'data': 'c', 'part': 'even'}
                ])

            # data frame insert by name should reorder columns
            spark.createDataFrame([(1, 'odd', 'g'), (3, 'odd', 'h')], ("id", "part", "data")).write.byName().insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(sort_by_id(rows), [
                    {'id': 1, 'data': 'g', 'part': 'odd'}, # note new data
                    {'id': 2, 'data': 'a', 'part': 'even'},
                    {'id': 3, 'data': 'h', 'part': 'odd'}, # note new data
                    {'id': 4, 'data': 'b', 'part': 'even'},
                    # note no id:5
                    {'id': 6, 'data': 'c', 'part': 'even'}
                ])


class ParquetUnpartitionedBatchPatternIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create a test table
        cls.shared_table = temp_table_name('parquet_partitioned')
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)
        sql("CREATE TABLE {0} (id bigint, data string) STORED AS parquet", cls.shared_table)
        sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", cls.shared_table)

    @classmethod
    def tearDownClass(cls):
        # clean up the test table
        sql("DROP TABLE IF EXISTS {0}", cls.shared_table)

    def test_simple_read(self):
        rows = collect(sql("select * from {0}", self.shared_table))
        self.assertEqual(rows, [
                {'id': 1, 'data': 'a'},
                {'id': 2, 'data': 'b'},
                {'id': 3, 'data': 'c'}
            ])

    def test_parquet_table_created(self):
        # describe produces a table with col_name, table_type, and comment columns, with properties mixed in after the schema
        describe = sql("DESCRIBE FORMATTED {0}", self.shared_table)

        types = list(filter(lambda r: r['col_name'].strip() == 'table_type', collect(describe)))
        self.assertEqual(len(types), 0, "Should produce no table_type property entry")

        formats = list(filter(lambda r: r['col_name'].strip() == 'InputFormat:', collect(describe)))
        self.assertEqual(len(formats), 1, "Should produce an InputFormat: property entry")
        self.assertEqual(
                formats[0]['data_type'].strip(),
                'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                "Should be a parquet table")

    @spark_only
    def test_batch_pattern_overwrite(self):
        with temp_table(
                "batch_overwrite_test",
                "CREATE TABLE {0} (id bigint, data string) STORED AS parquet") as t:

            # write some data as the first batch
            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 1, 'data': 'a'},
                    {'id': 2, 'data': 'b'},
                    {'id': 3, 'data': 'c'}
                ])

            # second insert should overwrite
            sql("INSERT INTO {0} VALUES (4, 'd'), (5, 'e'), (6, 'f')", t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 4, 'data': 'd'},
                    {'id': 5, 'data': 'e'},
                    {'id': 6, 'data': 'f'}
                ])

            # data frame insert should also overwrite
            spark.createDataFrame([(7, "g"), (8, "h"), (9, "i")], ("id", "data")).write.insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 7, 'data': 'g'},
                    {'id': 8, 'data': 'h'},
                    {'id': 9, 'data': 'i'}
                ])

            # test by name insert
            spark.createDataFrame([("j", 10), ("k", 11)], ("data", "id")).write.byName().insertInto(t)
            rows = collect(spark.table(t))
            self.assertEqual(rows, [
                    {'id': 10, 'data': 'j'},
                    {'id': 11, 'data': 'k'},
                ])



if __name__ == '__main__':
    exit_code = 0
    try:
        unittest.main()

    except Exception as e:
        traceback.print_exc()
        exit_code = -1

    finally:
        if _spark_accessed:
            spark.stop()

    SYS.exit(exit_code)
