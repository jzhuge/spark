SELECT 'SELECT iceberg.bdp_integration_tests.iceberg_dual', * FROM iceberg.bdp_integration_tests.iceberg_dual;

SELECT 'SELECT testiceberg.bdp_integration_tests.iceberg_dual', * FROM testiceberg.bdp_integration_tests.iceberg_dual;

SELECT 'SELECT iceberg.bdp_integration_tests.iceberg_dual', * FROM iceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'SELECT testiceberg.bdp_integration_tests.iceberg_dual', * FROM testiceberg.bdp_integration_tests.iceberg_dual;

SELECT 'SELECT default.dual', * FROM default.dual;

SELECT 'SELECT dual', * FROM dual;

INSERT OVERWRITE TABLE iceberg.bdp_integration_tests.iceberg_write
SELECT 'INSERT iceberg.bdp_integration_tests.iceberg_dual into iceberg.bdp_integration_tests.iceberg_write', *
  FROM iceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT testiceberg.bdp_integration_tests.iceberg_dual into iceberg.bdp_integration_tests.iceberg_write', *
  FROM testiceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT dual into iceberg.bdp_integration_tests.iceberg_write', * FROM dual;

SELECT * FROM iceberg.bdp_integration_tests.iceberg_write;

INSERT OVERWRITE TABLE testiceberg.bdp_integration_tests.iceberg_write
SELECT 'INSERT iceberg.bdp_integration_tests.iceberg_dual into testiceberg.bdp_integration_tests.iceberg_write', *
  FROM iceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT testiceberg.bdp_integration_tests.iceberg_dual into testiceberg.bdp_integration_tests.iceberg_write', *
  FROM testiceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT dual into testiceberg.bdp_integration_tests.iceberg_write', * FROM dual;

SELECT * FROM testiceberg.bdp_integration_tests.iceberg_write;

INSERT OVERWRITE TABLE bdp_integration_tests.iceberg_write
SELECT 'INSERT iceberg.bdp_integration_tests.iceberg_dual to bdp_integration_tests.iceberg_write', *
  FROM iceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT testiceberg.bdp_integration_tests.iceberg_dual to bdp_integration_tests.iceberg_write', *
  FROM testiceberg.bdp_integration_tests.iceberg_dual
UNION
SELECT 'INSERT default.dual to bdp_integration_tests.iceberg_write', * FROM default.dual;

SELECT * FROM bdp_integration_tests.iceberg_write;

