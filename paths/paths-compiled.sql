-- In-database representation of a file system

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

-- In database representation of a file system
-- Based on: https://stackoverflow.com/questions/18789502/how-to-solve-this-issue-with-cte

-- Beginning at a directory, we begin backtracking towards the root directory
-- concatenating the file path in the process.

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_paths (in_dir text, in_file_path text, val text, PRIMARY KEY(in_dir, in_file_path));

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
DROP TYPE IF EXISTS lifted_args CASCADE;
DROP TYPE IF EXISTS args CASCADE;
CREATE TYPE args AS (dir text, file_path text);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS file_path(text, text);
CREATE FUNCTION file_path(dir text, file_path text)
RETURNS text AS $$

--  BEGIN GENERATED CODE

    --  ITERATE
  WITH RECURSIVE call_graph(in_dir, in_file_path, site, fanout, out_dir, out_file_path, val) AS (
    SELECT file_path.dir, file_path.file_path, edges.*
    FROM (
      WITH
      memo(site, fanout, dir, file_path, val) AS (
        SELECT NULL :: int, 0, file_path.dir, file_path.file_path, memo.val
        FROM memoization_paths AS memo
        WHERE (file_path.dir, file_path.file_path) = (memo.in_dir, memo.in_file_path)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN (SELECT d.PARENT_DIR_ID FROM DIRS AS d WHERE d.DIR_NAME = file_path.dir) IS NULL THEN (NULL, false) :: lifted_args
            ELSE (row((
              SELECT d2.DIR_NAME
              FROM DIRS AS d, DIRS AS d2
              WHERE d.DIR_NAME = file_path.dir
              AND d.PARENT_DIR_ID = d2.DIR_ID),
              '/'||file_path.dir||file_path.file_path), true) :: lifted_args
          END)
      ),
      calls(site, fanout, dir, file_path, val) AS (
        SELECT s.site, 1, (s.out).args.dir, (s.out).args.file_path, NULL :: text
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, file_path.dir, file_path.file_path, (
        SELECT CASE
          WHEN (SELECT d.PARENT_DIR_ID FROM DIRS AS d WHERE d.DIR_NAME = file_path.dir) IS NULL THEN '/'||file_path.dir||file_path.file_path
          ELSE NULL :: text
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, dir, file_path, val)

      UNION ALL

    SELECT g.out_dir, g.out_file_path, edges.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g, LATERAL (
      WITH
      memo(site, fanout, dir, file_path, val) AS (
        SELECT NULL :: int, 0, g.out_dir, g.out_file_path, memo.val
        FROM memoization_paths AS memo
        WHERE (g.out_dir, g.out_file_path) = (memo.in_dir, memo.in_file_path)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN (SELECT d.PARENT_DIR_ID FROM DIRS AS d WHERE d.DIR_NAME = g.out_dir) IS NULL THEN (NULL, false) :: lifted_args
            ELSE (row((
              SELECT d2.DIR_NAME
              FROM DIRS AS d, DIRS AS d2
              WHERE d.DIR_NAME = g.out_dir
              AND d.PARENT_DIR_ID = d2.DIR_ID),
              '/'||g.out_dir||g.out_file_path), true) :: lifted_args
          END)
      ),
      calls(site, fanout, dir, file_path, val) AS (
        SELECT s.site, 1, (s.out).args.dir, (s.out).args.file_path, NULL :: text
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_dir, g.out_file_path, (
        SELECT CASE
          WHEN (SELECT d.PARENT_DIR_ID FROM DIRS AS d WHERE d.DIR_NAME = g.out_dir) IS NULL THEN '/'||g.out_dir||g.out_file_path
          ELSE NULL :: text
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, dir, file_path, val)
  ),
  base_cases(in_dir, in_file_path, val) AS (
    SELECT g.in_dir, g.in_file_path, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  memo AS (
    INSERT INTO memoization_paths ( SELECT DISTINCT file_path.dir, file_path.file_path, b.val FROM base_cases AS b ) ON CONFLICT DO NOTHING
  )
  SELECT b.val
  FROM base_cases AS b;

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Produce the absolute paths of all directories in the hierarchy

SELECT d.DIR_NAME, file_path(d.DIR_NAME, '') AS PATH
FROM   DIRS AS d;
