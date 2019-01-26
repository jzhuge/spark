#!/usr/bin/env python3

import sys
import traceback
import unittest

from test_utils import *


class IcebergVaultTest(unittest.TestCase):
    """Test Iceberg tables in vault.
    """
    def test_parquet_write(self):
        with temp_vault_table(
                "iceberg_parquet_write",
                "CREATE TABLE {0} (id bigint, data string) USING iceberg") as t:

            sql("INSERT INTO {0} VALUES (1, 'a'), (2, 'b'), (3, 'c')", t)

            self.assertEqual(collect(table(t)), [
                {'id': 1, 'data': 'a'},
                {'id': 2, 'data': 'b'},
                {'id': 3, 'data': 'c'}
            ])


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
