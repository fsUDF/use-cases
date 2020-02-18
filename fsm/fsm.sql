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

-- Recursive SQL UDF to parse string 'input'.  Finite state machine
-- currently is in state 'state'.
--
DROP FUNCTION IF EXISTS parse(int, text);
CREATE FUNCTION parse(state int, input text) RETURNS boolean AS
$$
  SELECT CASE WHEN length(input) = 0
              THEN (SELECT DISTINCT edge.final
                    FROM   fsm AS edge
                    WHERE  state = edge.source)
              ELSE COALESCE(parse((
                SELECT edge.target
                FROM fsm AS edge
                WHERE state = edge.source
                AND   strpos(edge.labels, left(input, 1)) > 0
              ), right(input, -1)), false)
         END;
$$ LANGUAGE SQL;

-----------------------------------------------------------------------
-- Parse 1000 chemical compounds and validate their formulae

SELECT c.*, parse(0, c.formula)
FROM   compounds AS c
LIMIT  1000;
