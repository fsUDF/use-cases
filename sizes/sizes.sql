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

DROP FUNCTION IF EXISTS file_size(int[], int);
CREATE FUNCTION file_size(next int[], file_size int)
RETURNS int AS $$
  SELECT CASE
    WHEN cardinality(next) = 0 THEN file_size
    ELSE file_size(
      next[2:]  || (SELECT ARRAY_AGG(d.DIR_ID) FROM DIRS AS d WHERE d.PARENT_DIR_ID = next[1]),
      file_size +  COALESCE((SELECT SUM(f.FILE_SIZE) FROM FILES AS f WHERE f.DIR_ID = next[1])::int, 0)
    )
  END;
$$ LANGUAGE SQL STABLE STRICT;

-----------------------------------------------------------------------
-- Compute the total file size of all files under a directory

SELECT d.DIR_NAME AS dir, file_size(ARRAY[d.DIR_ID], 0) AS "total size (in MB)"
FROM   DIRS AS d;
