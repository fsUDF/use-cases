-- Recursive interpreter for artihmetic expressions,
-- walks an expression DAG over +/* operators and numeric literals
--
-- Note: Function eval() defines a standard top-down evaluation, but
--       the evaluation AFTER COMPILATION to WITH RECURSIVE will perform
--       *parallel* bottom-up evaluation:
--       *all* literal leaves will be evaluated in one iteration,
--       in the next iteration *all* operators referring to these leaves
--       will be evaluated, etc.

\set n 10 -- generated DAG has size up to 2ⁿ nodes

DROP TABLE IF EXISTS expression CASCADE;

-- Expression representation (DAG)
CREATE TABLE expression(
  node int PRIMARY KEY,   -- expression node ID
  op   char(1),           -- operator ∊ {+,*} or ℓ (≡ literal)
  arg1 int,               -- left and
  arg2 int,               --   right arguments of operator (node = arg1 = arg2 if literal)
  lit  numeric,           -- literal value (0 if operator)
  FOREIGN KEY (arg1) REFERENCES expression(node),
  FOREIGN KEY (arg2) REFERENCES expression(node));

-- Make the expression row type hashable and comparable
--
\i expressions.sql

-----------------------------------------------------------------------
-- Random expression generation

SELECT setseed(0.42);

-- Randomly select an operator
DROP FUNCTION IF EXISTS oper();
CREATE FUNCTION oper() RETURNS char(1) AS
$$
  SELECT (array['+','*'])[1 + random()];
$$ LANGUAGE SQL;


INSERT INTO expression(node, op, arg1, arg2, lit)
  WITH RECURSIVE
  -- (1) construct inner operation nodes of expression tree
  operations(n, node, op, arg1, arg2, lit) AS (
    SELECT :n AS n, 0 AS node, oper() AS op, 0 * 2 + 1 AS arg1, 0 * 2 + 2 AS arg2, 0::numeric AS lit

      UNION ALL

    SELECT e.n - 1 AS n, n.node, n.op, n.arg1, n.arg2, n.lit
    FROM   operations AS e,
           LATERAL (VALUES (e.node * 2 + 1, oper(), (e.node * 2 + 1) * 2 + 1, (e.node * 2 + 1) * 2 + 2, 0::numeric),
                           (e.node * 2 + 2, oper(), (e.node * 2 + 2) * 2 + 1, (e.node * 2 + 2) * 2 + 2, 0::numeric)) AS n(node,op,arg1,arg2,lit)
    WHERE  e.n > 0
  ),
  -- (2) at the leaf level add literal expressions
  expr(node, op, arg1, arg2, lit) AS (
    SELECT  e.node, e.op, e.arg1, e.arg2, e.lit
    FROM    operations AS e
    WHERE   e.n > 0

      UNION

    SELECT  e.node, 'ℓ' AS op, e.node AS arg1, e.node AS arg2,
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
  TABLE maintree;


--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_eval (in_node int, in_op char(1), in_arg1 int, in_arg2 int, in_lit numeric, val numeric, PRIMARY KEY(in_node, in_op, in_arg1, in_arg2, in_lit));

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
DROP TYPE IF EXISTS lifted_args;
DROP TYPE IF EXISTS args;
CREATE TYPE args AS (node int, op char(1), arg1 int, arg2 int, lit numeric);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS eval(expression);
CREATE FUNCTION eval(e expression) RETURNS numeric AS
$$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_node, in_op, in_arg1, in_arg2, in_lit, site, fanout, out_node, out_op, out_arg1, out_arg2, out_lit, val) AS (
    SELECT (eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit, edges.*
    FROM (
      WITH
      memo(call_site, fanout, node, op, arg1, arg2, lit, val) AS (
        SELECT NULL :: int, 0, (eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit, m.val
        FROM memoization_eval AS m
        WHERE ((eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit) = (m.in_node, m.in_op, m.in_arg1, m.in_arg2, m.in_lit)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE (eval.e).op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN ROW((SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) :: args FROM expression AS e1 WHERE e1.node = (eval.e).arg1), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE (eval.e).op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN ROW((SELECT (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit) :: args FROM expression AS e2 WHERE e2.node = (eval.e).arg2), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE (eval.e).op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN (NULL, false) :: lifted_args
            WHEN '*' THEN ROW((SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) :: args FROM expression AS e1 WHERE e1.node = (eval.e).arg1), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 4, (
          SELECT CASE (eval.e).op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN (NULL, false) :: lifted_args
            WHEN '*' THEN ROW((SELECT (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit) :: args FROM expression AS e2 WHERE e2.node = (eval.e).arg2), true) :: lifted_args
          END
        )
      ),
      calls(site, fanout, node, op, arg1, arg2, lit, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.node, (s.out).args.op, (s.out).args.arg1, (s.out).args.arg2, (s.out).args.lit, NULL :: numeric
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, (eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit, (
        SELECT CASE (eval.e).op
          WHEN 'ℓ' THEN (eval.e).lit
          WHEN '+' THEN NULL :: numeric
                        +
                        NULL :: numeric
          WHEN '*' THEN NULL :: numeric
                        *
                        NULL :: numeric
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, node, op, arg1, arg2, lit, val)

      UNION

    SELECT g.out_node, g.out_op, g.out_arg1, g.out_arg2, g.out_lit, edges.*
    FROM call_graph AS g,
      LATERAL (
      WITH
      memo(call_site, fanout, node, op, arg1, arg2, lit, val) AS (
        SELECT NULL :: int, 0, g.out_node, g.out_op, g.out_arg1, g.out_arg2, g.out_lit, m.val
        FROM memoization_eval AS m
        WHERE (g.out_node, g.out_op, g.out_arg1, g.out_arg2, g.out_lit) = (m.in_node, m.in_op, m.in_arg1, m.in_arg2, m.in_lit)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE g.out_op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN ROW((SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) :: args FROM expression AS e1 WHERE e1.node = g.out_arg1), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE g.out_op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN ROW((SELECT (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit) :: args FROM expression AS e2 WHERE e2.node = g.out_arg2), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE g.out_op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN (NULL, false) :: lifted_args
            WHEN '*' THEN ROW((SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) :: args FROM expression AS e1 WHERE e1.node = g.out_arg1), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 4, (
          SELECT CASE g.out_op
            WHEN 'ℓ' THEN (NULL, false) :: lifted_args
            WHEN '+' THEN (NULL, false) :: lifted_args
            WHEN '*' THEN ROW((SELECT (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit) :: args FROM expression AS e2 WHERE e2.node = g.out_arg2), true) :: lifted_args
          END
        )
      ),
      calls(site, fanout, node, op, arg1, arg2, lit, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.node, (s.out).args.op, (s.out).args.arg1, (s.out).args.arg2, (s.out).args.lit, NULL :: numeric
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_node, g.out_op, g.out_arg1, g.out_arg2, g.out_lit, (
        SELECT CASE g.out_op
          WHEN 'ℓ' THEN g.out_lit
          WHEN '+' THEN NULL :: numeric
                        +
                        NULL :: numeric
          WHEN '*' THEN NULL :: numeric
                        *
                        NULL :: numeric
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, node, op, arg1, arg2, lit, val)
  ),
  base_cases(in_node, in_op, in_arg1, in_arg2, in_lit, val) AS (
    SELECT g.in_node, g.in_op, g.in_arg1, g.in_arg2, g.in_lit, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_node, in_op, in_arg1, in_arg2, in_lit, val) AS (
      TABLE base_cases
        UNION ALL (
      WITH e AS (TABLE evaluation),
      returns(in_node, in_op, in_arg1, in_arg2, in_lit, val) AS (
        SELECT go.in_node, go.in_op, go.in_arg1, go.in_arg2, go.in_lit, (
          SELECT CASE go.in_op
            WHEN 'ℓ' THEN go.in_lit
            WHEN '+' THEN go.ret[1]
                          +
                          go.ret[2]
            WHEN '*' THEN go.ret[3]
                          *
                          go.ret[4]
          END
        )
        FROM (
          SELECT   g.in_node, g.in_op, g.in_arg1, g.in_arg2, g.in_lit, array_gather(g.e_val, g.site) AS ret
          FROM     (SELECT g.*, e.val AS e_val FROM call_graph AS g, e WHERE (g.out_node, g.out_op, g.out_arg1, g.out_arg2, g.out_lit) = (e.in_node, e.in_op, e.in_arg1, e.in_arg2, e.in_lit) OFFSET 0) AS g
          WHERE    NOT EXISTS (SELECT 1 FROM e WHERE (e.in_node, e.in_op, e.in_arg1, e.in_arg2, e.in_lit) = (g.in_node, g.in_op, g.in_arg1, g.in_arg2, g.in_lit))
          GROUP BY g.in_node, g.in_op, g.in_arg1, g.in_arg2, g.in_lit, g.fanout
          HAVING   COUNT(*) = g.fanout
        ) AS go(in_node, in_op, in_arg1, in_arg2, in_lit, ret)
      )
      SELECT results.*
      FROM ( TABLE e UNION ALL TABLE returns ) AS results(in_node, in_op, in_arg1, in_arg2, in_lit, val)
      WHERE NOT EXISTS (SELECT 1 FROM e WHERE (e.in_node, e.in_op, e.in_arg1, e.in_arg2, e.in_lit) = ((eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit))
    )
  ),
  memo AS (
    INSERT INTO memoization_eval ( SELECT DISTINCT e.in_node, e.in_op, e.in_arg1, e.in_arg2, e.in_lit, e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_node, e.in_op, e.in_arg1, e.in_arg2, e.in_lit) = ((eval.e).node, (eval.e).op, (eval.e).arg1, (eval.e).arg2, (eval.e).lit);

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Perform expression evaluation of root expression
\set expr 0

SELECT e.node AS node, eval(e) AS result
FROM   expression AS e
WHERE  e.node = :expr;
