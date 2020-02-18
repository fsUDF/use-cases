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

-- Recursive SQL UDF in functional style: expression interpreter
DROP FUNCTION IF EXISTS eval(expression);
CREATE FUNCTION eval(e expression) RETURNS numeric AS
$$
  SELECT CASE e.op
    WHEN 'ℓ' THEN e.lit
    WHEN '+' THEN eval((SELECT e1 FROM expression AS e1 WHERE e1.node = e.arg1))
                  +
                  eval((SELECT e2 FROM expression AS e2 WHERE e2.node = e.arg2))
    WHEN '*' THEN eval((SELECT e1 FROM expression AS e1 WHERE e1.node = e.arg1))
                  *
                  eval((SELECT e2 FROM expression AS e2 WHERE e2.node = e.arg2))
  END;
$$ LANGUAGE SQL;

-----------------------------------------------------------------------
-- Perform expression evaluation of root expression
\set expr 0

SELECT e.node AS node, eval(e) AS result
FROM   expression AS e
WHERE  e.node = :expr;
