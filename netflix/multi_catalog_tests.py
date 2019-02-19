#!/usr/bin/env python3
import sys
import traceback
import unittest

from pyspark.sql.functions import lit

from test_utils import *


class MultiCatalogSelectTest(unittest.TestCase):

    def test_select(self):
        self.assertEqual(collect(sql("SELECT * FROM iceberg.bdp_integration_tests.iceberg_dual")), [{'foo': 'prodhive'}])
        self.assertEqual(collect(sql("SELECT * FROM testiceberg.bdp_integration_tests.iceberg_dual")), [{'foo': 'testhive'}])

        self.assertEqual(collect(table("iceberg.bdp_integration_tests.iceberg_dual")), [{'foo': 'prodhive'}])
        self.assertEqual(collect(table("testiceberg.bdp_integration_tests.iceberg_dual")), [{'foo': 'testhive'}])

    def test_join(self):
        self.assertEqual(collect(sql("""\
                SELECT *
                FROM dual
                CROSS JOIN testiceberg.bdp_integration_tests.iceberg_dual""")), [
            {'foo': "prodhive", 'foo': 'testhive'}
        ])
        self.assertEqual(collect(sql("""\
                SELECT *
                FROM iceberg.bdp_integration_tests.iceberg_dual
                CROSS JOIN dual""")), [
            {'foo': 'prodhive', 'foo': "prodhive"}
        ])
        self.assertEqual(collect(sql("""\
                SELECT *
                FROM iceberg.bdp_integration_tests.iceberg_dual
                CROSS JOIN testiceberg.bdp_integration_tests.iceberg_dual""")), [
            {'foo': 'prodhive', 'foo': 'testhive'}
        ])

    def test_select_session_catalog(self):
        """Ensure session catalog still works"""

        self.assertEqual(collect(sql("SELECT * FROM default.dual")), [{'foo': "prodhive"}])
        self.assertEqual(collect(sql("SELECT * FROM dual")), [{'foo': "prodhive"}])

        self.assertEqual(collect(table("default.dual")), [{'foo': "prodhive"}])
        self.assertEqual(collect(table("dual")), [{'foo': "prodhive"}])


class MultiCatalogInsertTest(unittest.TestCase):

    expected_rows = [
        {'id': '1', 'foo': 'prodhive'},
        {'id': '2', 'foo': 'testhive'},
        {'id': '3', 'foo': "prodhive"}
    ]

    target_table_list = [
        "iceberg.bdp_integration_tests.iceberg_write",
        "testiceberg.bdp_integration_tests.iceberg_write",
        "bdp_integration_tests.iceberg_write"
    ]

    @staticmethod
    def sql_insert_rows(t):
        sql("""\
            INSERT OVERWRITE TABLE {}
            SELECT '1', * FROM iceberg.bdp_integration_tests.iceberg_dual
            UNION
            SELECT '2', * FROM testiceberg.bdp_integration_tests.iceberg_dual
            UNION
            SELECT '3', * FROM dual""".format(t))

    def test_sql_insert(self):
        for t in self.target_table_list:
            print("Testing INSERT OVERWRITE table {}".format(t))
            self.sql_insert_rows(t)
            self.assertEqual(collect(table(t)), self.expected_rows)

    def test_df_insert(self):
        df1 = table("iceberg.bdp_integration_tests.iceberg_dual").withColumn("id", lit("1"))
        df2 = table("testiceberg.bdp_integration_tests.iceberg_dual").withColumn("id", lit("2"))
        df3 = table("default.dual").withColumn("id", lit("3"))
        df = df1.union(df2).union(df3).select("id", "foo")

        for t in self.target_table_list:
            print("Testing insertInto table {}".format(t))
            df.write.insertInto(t, overwrite=True)
            self.assertEqual(sorted(collect(table(t))), sorted(self.expected_rows))


if __name__ == '__main__':
    exit_code = 0
    try:
        unittest.main()

    except Exception as e:
        traceback.print_exc()
        exit_code = -1

    finally:
        spark.stop()

    sys.exit(exit_code)
