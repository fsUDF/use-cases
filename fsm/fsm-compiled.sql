-- Demonstrate the use of a recursive SQL UDF to drive a finite state
-- machine (derived from a regular expression) to parse chemical compound
-- formulae.

-- Note: Formulae that share a common suffic should benefit from
--       memoization during parsing: the result of parsing a
--       suffix we have seen before is found in the memoization table
--       (and the suffix will not be re-traversed).

---------------------------------------------------------------------
-- Relational representation of finite state machine

DROP DOMAIN IF EXISTS state CASCADE;
CREATE DOMAIN state AS integer;

-- Transition table
--
DROP TABLE IF EXISTS fsm;
CREATE TABLE fsm (
  source  state NOT NULL, -- source state of transition
  labels  text  NOT NULL, -- transition labels (input)
  target  state,          -- target state of transition
  final   boolean,        -- is source a final state?
  PRIMARY KEY (source, labels)
);

-- Create DFA transition table for regular expression
-- ([A-Za-z]+[₀-₉]*([⁰-⁹]*[⁺⁻])?)+
--
INSERT INTO fsm(source, labels, target, final) VALUES
  (0, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',           1, false ),
  (1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz₀₁₂₃₄₅₆₇₈₉', 1, true ),
  (1, '⁰¹²³⁴⁵⁶⁷⁸⁹',                                                     2, true),
  (1, '⁺⁻',                                                             3, true ),
  (2, '⁰¹²³⁴⁵⁶⁷⁸⁹',                                                     2, false),
  (2, '⁺⁻',                                                             3, false ),
  (3, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',           1, true );



-- Table of chemical compounds
--
DROP TABLE IF EXISTS compounds;
CREATE TABLE compounds (
  compound text NOT NULL PRIMARY KEY,
  formula  text
);

-- Populate compounds table
--
\i compounds.sql

-- "Brew" additional compounds
--
INSERT INTO compounds
  SELECT c1.compound || '+' || c2.compound AS compound,
         c1.formula || c2.formula AS formula
  FROM   compounds AS c1, compounds AS c2;


--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_fsm (in_state int, in_input text, val boolean, PRIMARY KEY(in_state, in_input));

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
CREATE TYPE args AS (state int, input text);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS parse(int, text);
CREATE FUNCTION parse(state int, input text) RETURNS boolean AS
$$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_state, in_input, site, fanout, out_state, out_input, val) AS (
    SELECT parse.state, parse.input, edges.*
    FROM (
      WITH
      memo(site, fanout, state, input, val) AS (
        SELECT NULL :: int, 0, parse.state, parse.input, memo.val
        FROM memoization_fsm AS memo
        WHERE (parse.state, parse.input) = (memo.in_state, memo.in_input)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE WHEN length(parse.input) = 0
              THEN (NULL, false) :: lifted_args
              ELSE (((
                SELECT edge.target
                FROM fsm AS edge
                WHERE parse.state = edge.source
                AND   strpos(edge.labels, left(parse.input, 1)) > 0
              ), right(parse.input, -1)), true) :: lifted_args
         END
        )
      ),
      calls(site, fanout, state, input, val) AS (
        SELECT s.site, 1, (s.out).args.state, (s.out).args.input, NULL :: boolean
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, parse.state, parse.input, (
        SELECT CASE WHEN length(parse.input) = 0
              THEN (SELECT DISTINCT edge.final
                    FROM   fsm AS edge
                    WHERE  parse.state = edge.source)
              ELSE COALESCE(NULL :: boolean, false)
         END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, state, input, val)

      UNION ALL

    SELECT g.out_state, g.out_input, edges.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g,
      LATERAL (
        WITH
        memo(site, fanout, state, input, val) AS (
          SELECT NULL :: int, 0, g.out_state, g.out_input, memo.val
          FROM memoization_fsm AS memo
          WHERE (g.out_state, g.out_input) = (memo.in_state, memo.in_input)
        ),
        slices(site, out) AS (
          SELECT 1, (
            SELECT CASE WHEN length(g.out_input) = 0
                THEN (NULL, false) :: lifted_args
                ELSE (((
                  SELECT edge.target
                  FROM fsm AS edge
                  WHERE g.out_state = edge.source
                  AND   strpos(edge.labels, left(g.out_input, 1)) > 0
                ), right(g.out_input, -1)), true) :: lifted_args
           END
          )
        ),
        calls(site, fanout, state, input, val) AS (
          SELECT s.site, 1, (s.out).args.state, (s.out).args.input, NULL :: boolean
          FROM   slices AS s
          WHERE  (s.out).not_bottom
        )
        TABLE memo
          UNION ALL
        SELECT *
        FROM   calls
        WHERE  NOT EXISTS (TABLE memo)
          UNION ALL
        SELECT NULL :: int, 0, g.out_state, g.out_input, (
          SELECT CASE WHEN length(g.out_input) = 0
                THEN (SELECT DISTINCT edge.final
                      FROM   fsm AS edge
                      WHERE  g.out_state = edge.source)
                ELSE COALESCE(NULL :: boolean, false)
           END
        )
        WHERE  NOT EXISTS (TABLE memo)
        AND    NOT EXISTS (TABLE calls)
      ) AS edges(fanout, state, input, val)
  ),
  base_cases(in_state, in_input, val) AS (
    SELECT g.in_state, g.in_input, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_state, in_input, val) AS (
    TABLE base_cases
      UNION
    SELECT go.in_state, go.in_input, (
      SELECT CASE WHEN length(go.in_input) = 0
                  THEN (SELECT DISTINCT edge.final
                        FROM   fsm AS edge
                        WHERE  go.in_state = edge.source)
                  ELSE COALESCE(go.val, false)
             END
        )
    FROM (
      SELECT g.in_state, g.in_input, e.val
      FROM   call_graph AS g, evaluation AS e
      WHERE  (g.out_state, g.out_input) = (e.in_state, e.in_input)
    ) AS go(in_state, in_input, val)
  ),
  memo AS (
    INSERT INTO memoization_fsm ( SELECT DISTINCT e.in_state, e.in_input, e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_state, e.in_input) = (parse.state, parse.input);

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Parse 1000 chemical compounds and validate their formulae

SELECT c.*, parse(0, c.formula)
FROM   compounds AS c
LIMIT  1000;
