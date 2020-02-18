-- Connected components in a directed acyclic graph (DAG) with an out-degree of two

\set n 10 -- generated DAG has size up to 2ⁿ nodes

DROP TABLE IF EXISTS nodes CASCADE;
DROP TYPE IF EXISTS child CASCADE;

-- Uniquely marks the edge connecting a node n to its children.
--
CREATE TYPE child AS ENUM ('l', 'r');

-- Each node is defined by its id and has at most two children.
-- This is enforced by having id and child be a primary key.
--
--     n
--   ↙   ↘
--  l     r
--
CREATE TABLE nodes (
  id    int,
  child child,
  next  int,
  PRIMARY KEY(id, child)
);

SELECT setseed(0.41);

-- Randomly select an operator
DROP FUNCTION IF EXISTS oper();
CREATE FUNCTION oper() RETURNS char(1) AS
$$
  SELECT (array['+','*'])[1 + random()];
$$ LANGUAGE SQL;

INSERT INTO nodes
WITH RECURSIVE
-- (1) construct inner operation nodes of expression tree
operations(n, node, op, arg1, arg2, lit) AS (
  SELECT :n AS n, 0 AS node, oper() AS op, 0 * 2 + 1 AS arg1, 0 * 2 + 2 AS arg2, NULL::numeric AS lit

    UNION ALL

  SELECT e.n - 1 AS n, n.node, n.op, n.arg1, n.arg2, n.lit
  FROM   operations AS e,
         LATERAL (VALUES (e.node * 2 + 1, oper(), (e.node * 2 + 1) * 2 + 1, (e.node * 2 + 1) * 2 + 2, NULL::numeric),
                         (e.node * 2 + 2, oper(), (e.node * 2 + 2) * 2 + 1, (e.node * 2 + 2) * 2 + 2, NULL::numeric)) AS n(node,op,arg1,arg2,lit)
  WHERE  e.n > 0
),
-- (2) at the leaf level add literal expressions
expr(node, op, arg1, arg2, lit) AS (
  SELECT  e.node, e.op, e.arg1, e.arg2, e.lit
  FROM    operations AS e
  WHERE   e.n > 0

    UNION

  SELECT  e.node, 'ℓ' AS op, NULL AS arg1, NULL AS arg2,
          100 * random() AS lit
  FROM    operations AS e
  WHERE   e.n = 0
),
-- (3) introduce distortion and sharing into the expression, we end up with a DAG
shuffle(node, op, arg1, arg2, lit) AS (
  SELECT e.node, e.op,
         CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                  ELSE e.arg1
         END AS arg1,
         CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                  ELSE e.arg2
         END AS arg2,
         e.lit
  FROM   expr AS e
  WHERE  e.op <> 'ℓ'

    UNION

  SELECT e.*
  FROM   expr AS e
  WHERE  e.op = 'ℓ'
),
maintree AS (
  SELECT *
  FROM   shuffle AS s
  WHERE  s.node = 0
    UNION
  SELECT s.*
  FROM   maintree AS m,
         LATERAL (SELECT *
                  FROM   shuffle AS s
                  WHERE  m.arg1 = s.node
                    UNION ALL
                  SELECT *
                  FROM   shuffle AS s
                  WHERE  m.arg2 = s.node) AS s
)
SELECT s.node AS id, 'l' :: child, s.arg1
FROM   maintree AS s
WHERE  NOT s.arg1 IS NULL
  UNION ALL
SELECT s.node AS id, 'r' :: child, s.arg2
FROM   maintree AS s
WHERE  NOT s.arg2 IS NULL;

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_comps (in_node int, in_target int, val boolean, PRIMARY KEY(in_node, in_target));

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
CREATE TYPE args AS (node int);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS connected(int, int);
CREATE FUNCTION connected(node int, target int)
RETURNS boolean AS $$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_node, site, fanout, out_node, val) AS (
    SELECT connected.node, edges.*
    FROM (
      WITH
      memo(call_site, fanout, node, val) AS (
        SELECT NULL :: int, 0, connected.node, m.val
        FROM memoization_comps AS m
        WHERE (connected.node, connected.target) = (m.in_node, m.in_target)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN connected.node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = connected.node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = connected.node) = 2 THEN
              (ROW((SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (connected.node, 'l'))), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN connected.node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = connected.node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = connected.node) = 2 THEN
              (ROW((SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (connected.node, 'r'))), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN connected.node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = connected.node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = connected.node) = 2 THEN (NULL, false) :: lifted_args
            ELSE
              (ROW((SELECT n.next FROM nodes AS n WHERE n.id = connected.node)), true) :: lifted_args
          END
        )
      ),
      calls(site, fanout, node, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.node, NULL :: boolean
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, connected.node, (
        SELECT CASE
          WHEN connected.node = connected.target THEN TRUE
          WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = connected.node) THEN FALSE
          WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = connected.node) = 2 THEN
            NULL :: boolean OR NULL :: boolean
          ELSE NULL :: boolean
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, node, val)

      UNION

    SELECT g.out_node, edges.*
    FROM call_graph AS g,
      LATERAL (
      WITH
      memo(call_site, fanout, node, val) AS (
        SELECT NULL :: int, 0, g.out_node, m.val
        FROM memoization_comps AS m
        WHERE (g.out_node, connected.target) = (m.in_node, m.in_target)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN g.out_node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = g.out_node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = g.out_node) = 2 THEN
              (ROW((SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (g.out_node, 'l'))), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN g.out_node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = g.out_node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = g.out_node) = 2 THEN
              (ROW((SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (g.out_node, 'r'))), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN g.out_node = connected.target THEN (NULL, false) :: lifted_args
            WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = g.out_node) THEN (NULL, false) :: lifted_args
            WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = g.out_node) = 2 THEN (NULL, false) :: lifted_args
            ELSE
              (ROW((SELECT n.next FROM nodes AS n WHERE n.id = g.out_node)), true) :: lifted_args
          END
        )
      ),
      calls(site, children, node, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.node, NULL :: boolean
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_node, (
        SELECT CASE
          WHEN g.out_node = connected.target THEN TRUE
          WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = g.out_node) THEN FALSE
          WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = g.out_node) = 2 THEN
            NULL :: boolean OR NULL :: boolean
          ELSE NULL :: boolean
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, node, val)
  ),
  base_cases(in_node, val) AS (
    SELECT g.in_node, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_node, val) AS (
    TABLE base_cases
      UNION ALL (
    WITH e AS (TABLE evaluation),
    returns(in_node, val) AS (
      SELECT go.in_node, (
        SELECT CASE
          WHEN go.in_node = connected.target THEN TRUE
          WHEN NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = go.in_node) THEN FALSE
          WHEN (SELECT COUNT(*) FROM nodes AS n WHERE n.id = go.in_node) = 2 THEN
            go.ret[1] OR go.ret[2]
          ELSE go.ret[3]
        END
      )
      FROM (
        SELECT   g.in_node, array_gather(g.e_val, g.site) AS ret
        FROM     (SELECT g.*, e.val AS e_val FROM call_graph AS g, e WHERE (g.out_node) = (e.in_node) OFFSET 0) AS g
        WHERE    NOT EXISTS (SELECT 1 FROM e WHERE (e.in_node) = (g.in_node))
        GROUP BY g.in_node, g.fanout
        HAVING   COUNT(*) = g.fanout
      ) AS go(in_node, ret)
    )
    SELECT results.*
    FROM ( TABLE e UNION ALL TABLE returns ) AS results(in_node, val)
    WHERE NOT EXISTS (SELECT 1 FROM e WHERE (e.in_node) = (connected.node))
  )),
  memo AS (
    INSERT INTO memoization_comps ( SELECT DISTINCT e.in_node, connected.target, e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_node) = (connected.node);

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Check whether node there can be reached from here

SELECT here, there, connected(here,there)
FROM   (SELECT n.id FROM nodes AS n ORDER BY random() LIMIT 1) AS _(here),
       (SELECT n.id FROM nodes AS n ORDER BY random() LIMIT 1) AS __(there);
