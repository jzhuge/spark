create temporary view t2 as select * from values 0, 1 as t(id);

-- WITH clause should reference the previous CTE
WITH t1 AS (SELECT * FROM t2), t2 AS (SELECT 2 FROM t1) SELECT * FROM t1 cross join t2;
