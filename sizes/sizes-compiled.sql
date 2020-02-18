-- In-database representation of a file system
-- Based on https://stackoverflow.com/questions/18789502/how-to-solve-this-issue-with-cte

-- A directory has a name and a parent directory through which it is defined.
-- A directory without a parent is the root directory.
DROP TABLE IF EXISTS DIRS CASCADE;
CREATE TABLE DIRS (
  DIR_ID        int PRIMARY KEY,
  PARENT_DIR_ID int REFERENCES DIRS(DIR_ID),
  DIR_NAME      text UNIQUE
);

CREATE INDEX ON DIRS(PARENT_DIR_ID);

-- A file has a name and size.
-- A file exists in a directory.
DROP TABLE IF EXISTS FILES CASCADE;
CREATE TABLE FILES (
  FILE_ID   int PRIMARY KEY,
  FILE_NAME text,
  DIR_ID    int REFERENCES DIRS(DIR_ID),
  FILE_SIZE int
);

SELECT setseed(0.42);

\set dir_count 1000          -- How many directories we generate
\set fanout 3                -- The maximum number of directories that can exist inside a directory
\set file_count :dir_count*2 -- The maximum number of files that can exist inside a directory

CREATE OR REPLACE FUNCTION random_dictionaries(n int, fanout double precision) RETURNS VOID AS
$$
DECLARE
  id  int := 1;
  min int := 1;
  max int := 1;
BEGIN
  INSERT INTO DIRS VALUES (id,NULL,'ROOT');
  WHILE max < n LOOP
    min := max + 1;
    max := LEAST(max + floor(random() * fanout + 1) :: int, n);
    FOR i in min..max LOOP
      INSERT INTO DIRS VALUES (i,id,'DIR'||i);
    END LOOP;
    id := id + 1;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

SELECT random_dictionaries(:dir_count, :fanout);

INSERT INTO FILES
SELECT file, 'test'||file::text||'.txt', floor(random() * :dir_count + 1) :: int, floor(random()*5+1) :: int * 100
FROM generate_series(1,:file_count) AS _(file);

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_sizes (in_next int[], in_file_size int, val int, PRIMARY KEY(in_next, in_file_size));

-- Add element x to array xs at position i
DROP AGGREGATE IF EXISTS array_gather(anyelement, int) CASCADE;
DROP FUNCTION IF EXISTS array_gather_step(anyarray, anyelement, int) CASCADE;
CREATE FUNCTION array_gather_step(xs anyarray, x anyelement, i int) RETURNS anyarray
AS $$
  BEGIN
    xs[i] := x;
    RETURN xs;
  END;
$$ LANGUAGE plpgsql STABLE;

-- Aggregate an array at fixed array positions. Any other undefined position is NULL
CREATE AGGREGATE array_gather(anyelement, int) (
  SFUNC       = array_gather_step,
  STYPE       = anyarray,
  INITCOND    = '{}'
);

-- (lifted) argument and result types
DROP TYPE IF EXISTS lifted_args  CASCADE;
DROP TYPE IF EXISTS args CASCADE;
CREATE TYPE args AS (next int[], file_size int);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

-- Sum up the file sizes of this and any underlying directories
DROP FUNCTION IF EXISTS file_size(int[], int);
CREATE FUNCTION file_size(next int[], file_size int)
RETURNS int AS $$

--  BEGIN GENERATED CODE

    --  ITERATE
  WITH RECURSIVE call_graph(in_next, in_file_size, site, fanout, out_next, out_file_size, val) AS (
    SELECT file_size.next, file_size.file_size, edges.*
    FROM (
      WITH
      memo(site, fanout, next, file_size, val) AS (
        SELECT NULL :: int, 0, file_size.next, file_size.file_size, memo.val
        FROM memoization_sizes AS memo
        WHERE (file_size.next, file_size.file_size) = (memo.in_next, memo.in_file_size)
      ),
      slices(site, out) AS (
        SELECT 1, (SELECT CASE
          WHEN cardinality(file_size.next) = 0 THEN (NULL, false) :: lifted_args
          ELSE (row(
            file_size.next[2:]  || (SELECT ARRAY_AGG(d.DIR_ID) FROM DIRS AS d WHERE d.PARENT_DIR_ID = file_size.next[1]),
            file_size.file_size +  COALESCE((SELECT SUM(f.FILE_SIZE) FROM FILES AS f WHERE f.DIR_ID = file_size.next[1])::int, 0)
          ), true) :: lifted_args
        END)
      ),
      calls(site, fanout, next, file_size, val) AS (
        SELECT s.site, 1, (s.out).args.next, (s.out).args.file_size, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, file_size.next, file_size.file_size, (
        SELECT CASE
          WHEN cardinality(file_size.next) = 0 THEN file_size.file_size
          ELSE NULL :: int
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, next, file_size, val)

      UNION ALL

    SELECT g.out_next, g.out_file_size, edges.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g, LATERAL (
      WITH
      memo(site, fanout, next, file_size, val) AS (
        SELECT NULL :: int, 0, g.out_next, g.out_file_size, m.val
        FROM memoization_sizes AS m
        WHERE (g.out_next, g.out_file_size) = (m.in_next, m.in_file_size)
      ),
      slices(site, out) AS (
        SELECT 1, (SELECT CASE
          WHEN cardinality(g.out_next) = 0 THEN (NULL, false) :: lifted_args
          ELSE (row(
            g.out_next[2:]  || (SELECT ARRAY_AGG(d.DIR_ID) FROM DIRS AS d WHERE d.PARENT_DIR_ID = g.out_next[1]),
            g.out_file_size +  COALESCE((SELECT SUM(f.FILE_SIZE) FROM FILES AS f WHERE f.DIR_ID = g.out_next[1])::int, 0)
          ), true) :: lifted_args
        END)
      ),
      calls(site, fanout, next, file_size, val) AS (
        SELECT s.site, 1, (s.out).args.next, (s.out).args.file_size, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_next, g.out_file_size, (
        SELECT CASE
          WHEN cardinality(g.out_next) = 0 THEN g.out_file_size
          ELSE NULL :: int
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, next, file_size, val)
  ),
  base_cases(in_next, in_file_size, val) AS (
    SELECT g.in_next, g.in_file_size, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  memo AS (
    INSERT INTO memoization_sizes ( SELECT DISTINCT file_size.next, file_size.file_size, b.val FROM base_cases AS b ) ON CONFLICT DO NOTHING
  )
  SELECT b.val
  FROM base_cases AS b;

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Compute the total file size of all files under a directory

SELECT d.DIR_NAME AS dir, file_size(ARRAY[d.DIR_ID], 0) AS "total size (in MB)"
FROM   DIRS AS d;
