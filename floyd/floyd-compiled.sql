-- Floyd-Warshall's algorithm to find the length of the shortest path
-- in a weighted, directed graph
--
-- The naive formulation below performs an exponential number of
-- recursive calls (O(3ⁿ) for a graph of n nodes).  Once the
-- UDF is compiled into a CTE and call sharing is applied, we observe
-- the expected O(n³) complexity of the Floyd-Warshall algorithm.
-- Additionally, over time, memoization leads to the materialization of
-- the graph's node distance matrix, which drastically cuts down
-- evaluation time.

-----------------------------------------------------------------------
-- Recursive SQL UDF in functional style that implements
-- Floyd-Warshall's algorithm to find the length of the
-- shortest path between nodes s and e (returns NULL if there
-- is no such path):
--

-- Graph size (# of nodes)
\set G 8

SELECT setseed(0.42);

-- Node set (densely numbered, arbitrary node labels)
DROP TABLE IF EXISTS nodes CASCADE;
CREATE TABLE nodes (
  node  int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  label char(1)
);

-- Generate G random nodes, label ∊ {'A'...'Z'}
INSERT INTO nodes(label)
  SELECT chr(ascii('A') + (random() * 25) :: int)
  FROM   generate_series(1, :G);

-- Edge set (weighted, directed)
DROP TABLE IF EXISTS edges CASCADE;
CREATE TABLE edges (
  here   int REFERENCES nodes,
  there  int REFERENCES nodes,
  weight int,
  PRIMARY KEY (here, there));

-- Generate random edges
INSERT INTO edges(here, there, weight)
  SELECT here.node AS here, there.node AS there,
         (random() * 10) :: int AS weight
  FROM   nodes AS here, nodes AS there
  WHERE  abs(ascii(here.label) - ascii(there.label)) <= 3
  AND    random() < 0.5;

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_floyd (in_nodes int, in_s int, in_e int, val int, PRIMARY KEY(in_nodes, in_s, in_e));

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
CREATE TYPE args AS (nodes int, s int, e int);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS shortestpath(int, int, int);
CREATE FUNCTION shortestpath(nodes int, s int, e int) RETURNS int AS
$$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_nodes, in_s, in_e, site, fanout, out_nodes, out_s, out_e, val) AS (
    SELECT shortestpath.nodes, shortestpath.s, shortestpath.e, edges.*
    FROM (
      WITH
      memo(call_site, fanout, nodes, s, e, val) AS (
        SELECT NULL :: int, 0, shortestpath.nodes, shortestpath.s, shortestpath.e, m.val
        FROM memoization_floyd AS m
        WHERE (shortestpath.nodes, shortestpath.s, shortestpath.e) = (m.in_nodes, m.in_s, m.in_e)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN shortestpath.nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((shortestpath.nodes - 1, shortestpath.s, shortestpath.e), true) :: lifted_args)
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN shortestpath.nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((shortestpath.nodes - 1, shortestpath.s, shortestpath.nodes), true) :: lifted_args)
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN shortestpath.nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((shortestpath.nodes - 1, shortestpath.nodes, shortestpath.e), true) :: lifted_args )
          END
        )
      ),
      calls(site, fanout, nodes, s, e, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.nodes, (s.out).args.s, (s.out).args.e, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, shortestpath.nodes, shortestpath.s, shortestpath.e, (
        SELECT CASE
          WHEN shortestpath.nodes = 0 THEN (SELECT edge.weight
                               FROM   edges AS edge
                               WHERE  (edge.here,edge.there) = (shortestpath.s,shortestpath.e))
          ELSE (SELECT LEAST(NULL :: int,
                             NULL :: int + NULL :: int))
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, nodes, s, e, val)

      UNION

    SELECT g.out_nodes, g.out_s, g.out_e, edges.*
    FROM call_graph AS g,
      LATERAL (
      WITH
      memo(call_site, fanout, nodes, s, e, val) AS (
        SELECT NULL :: int, 0, g.out_nodes, g.out_s, g.out_e, m.val
        FROM memoization_floyd AS m
        WHERE (g.out_nodes, g.out_s, g.out_e) = (m.in_nodes, m.in_s, m.in_e)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN g.out_nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((g.out_nodes - 1, g.out_s, g.out_e), true) :: lifted_args)
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN g.out_nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((g.out_nodes - 1, g.out_s, g.out_nodes), true) :: lifted_args)
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN g.out_nodes = 0 THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((g.out_nodes - 1, g.out_nodes, g.out_e), true) :: lifted_args )
          END
        )
      ),
      calls(site, fanout, nodes, s, e, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.nodes, (s.out).args.s, (s.out).args.e, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_nodes, g.out_s, g.out_e, (
        SELECT CASE
          WHEN g.out_nodes = 0 THEN (SELECT edge.weight
                               FROM   edges AS edge
                               WHERE  (edge.here,edge.there) = (g.out_s,g.out_e))
          ELSE (SELECT LEAST(NULL :: int,
                             NULL :: int + NULL :: int))
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, nodes, s, e, val)
  ),
  base_cases(in_nodes, in_s, in_e, val) AS (
    SELECT g.in_nodes, g.in_s, g.in_e, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_nodes, in_s, in_e, val) AS (
    TABLE base_cases
      UNION ALL (
    WITH e AS (TABLE evaluation),
    returns(in_nodes, in_s, in_e, val) AS (
      SELECT go.in_nodes, go.in_s, go.in_e, (
        SELECT CASE
          WHEN go.in_nodes = 0 THEN (SELECT edge.weight
                               FROM   edges AS edge
                               WHERE  (edge.here,edge.there) = (go.in_s,go.in_e))
          ELSE (SELECT LEAST(go.ret[1],
                             go.ret[2] + go.ret[3]))
        END
      )
      FROM (
        SELECT   g.in_nodes, g.in_s, g.in_e, array_gather(g.e_val, g.site) AS ret
        FROM     (SELECT g.*, e.val AS e_val FROM call_graph AS g, e WHERE (g.out_nodes, g.out_s, g.out_e) = (e.in_nodes, e.in_s, e.in_e) OFFSET 0) AS g
        WHERE    NOT EXISTS (SELECT 1 FROM e WHERE (e.in_nodes, e.in_s, e.in_e) = (g.in_nodes, g.in_s, g.in_e))
        GROUP BY g.in_nodes, g.in_s, g.in_e, g.fanout
        HAVING   COUNT(*) = g.fanout
      ) AS go(in_nodes, in_s, in_e, ret)
    )
    SELECT results.*
    FROM ( TABLE e UNION ALL TABLE returns ) AS results(in_nodes, in_s, in_e, val)
    WHERE NOT EXISTS (SELECT 1 FROM e WHERE (e.in_nodes, e.in_s, e.in_e) = (shortestpath.nodes, shortestpath.s, shortestpath.e))
  )),
  memo AS (
    INSERT INTO memoization_floyd ( SELECT DISTINCT e.in_nodes, e.in_s, e.in_e, e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_nodes, e.in_s, e.in_e) = (shortestpath.nodes, shortestpath.s, shortestpath.e);

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Find the length of the shortest path from here to there
\set here  1
\set there 5

SELECT :here AS here,
       :there AS there,
       shortestpath((SELECT COUNT(*) FROM nodes) :: int, :here, :there);
