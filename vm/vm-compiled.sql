-- Virtual machine (VM) featuring three-address opcodes

-- Currently supported VM instruction set
--
DROP TYPE IF EXISTS opcode CASCADE;
CREATE TYPE opcode AS ENUM (
  'lod',  -- lod t, x       load literal x into target register Rt
  'mov',  -- mov t, s       move from source register Rs to target register Rt
  'jeq',  -- jeq t, s, @a   if Rt = Rs, jump to location a, else fall through
  'jmp',  -- jmp @a         jump to location a
  'add',  -- add t, s1, s2  Rt ← Rs1 + Rs2
  'mul',  -- mul t, s1, s2  Rt ← Rs1 * Rs2
  'div',  -- div t, s1, s2  Rt ← Rs1 / Rs2
  'mod',  -- mod t, s1, s2  Rt ← Rs1 mod Rs2
  'hlt'   -- htl s          halt program, result is register Rs
);

-- A single VM instruction
--
DROP TYPE IF EXISTS instruction CASCADE;
CREATE TYPE instruction AS (
  loc   int,     -- location
  opc   opcode,  -- opcode
  reg1  int,     -- ┐
  reg2  int,     -- │ up to three work registers
  reg3  int      -- ┘
);

-- A program is a table of instructions
--
DROP TABLE IF EXISTS program CASCADE;
CREATE TABLE program OF instruction;

CREATE INDEX ip ON program USING btree (loc);

-----------------------------------------------------------------------
-- Program to compute the length of the Collatz sequence (also known as
-- the "3N + 1 problem") for the value N held in register R1.  Program
-- entry is at location 0.
--
INSERT INTO program(loc, opc, reg1, reg2, reg3) VALUES
  ( 0, 'lod', 4, 0   , NULL),
  ( 1, 'lod', 5, 1   , NULL),
  ( 2, 'lod', 6, 2   , NULL),
  ( 3, 'lod', 7, 3   , NULL),
  ( 4, 'mov', 2, 4   , NULL),
  ( 5, 'jeq', 1, 5   , 14  ),
  ( 6, 'add', 2, 2   , 5   ),
  ( 7, 'mod', 3, 1   , 6   ),
  ( 8, 'jeq', 3, 5   , 11  ),
  ( 9, 'div', 1, 1   , 6   ),
  (10, 'jmp', 5, NULL, NULL),
  (11, 'mul', 1, 1   , 7   ),
  (12, 'add', 1, 1   , 5   ),
  (13, 'jmp', 5, NULL, NULL),
  (14, 'hlt', 2, NULL, NULL);

-----------------------------------------------------------------------
-- Tail-recursive SQL UDF into functional style that implements
-- the VM instructions.  The second parameter represents the
-- machine's register file (each register hold one integer).
--

--  BEGIN INITIALIZATION

-- Memoization table
DROP TABLE IF EXISTS memoization_vm;
CREATE TABLE memoization_vm (in_ins instruction, in_regs int[], val int, PRIMARY KEY(in_ins, in_regs));

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
CREATE TYPE args AS (ins instruction, regs int[]);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS run(instruction, int[]);
CREATE FUNCTION run(ins instruction, regs int[]) RETURNS int AS
$$

--  BEGIN GENERATED CODE

    --  ITERATE
  WITH RECURSIVE call_graph(in_ins, in_regs, site, fanout, out_ins, out_regs, val) AS (
    SELECT run.ins, run.regs, calls.*
    FROM (
      WITH
      memo(site, fanout, ins, regs, val) AS (
        SELECT NULL :: int, 0, run.ins, run.regs, memo.val
        FROM memoization_vm AS memo
        WHERE (run.ins, run.regs) = (memo.in_ins, memo.in_regs)
      ),
      slices(site, out) AS (
        SELECT 1, (
        SELECT CASE (run.ins).opc
          WHEN 'lod' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || (run.ins).reg2 || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 2, (
        SELECT CASE (run.ins).opc
          WHEN 'mov' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || run.regs[(run.ins).reg2] || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 3, (
        SELECT CASE (run.ins).opc
          WHEN 'jeq' THEN (row((SELECT p FROM program AS p WHERE p.loc = CASE WHEN run.regs[(run.ins).reg1] = run.regs[(run.ins).reg2]
                                                                             THEN (run.ins).reg3
                                                                             ELSE (run.ins).loc + 1
                                                                        END),
                              run.regs), true) :: lifted_args
        END)
          UNION ALL
        SELECT 4, (
        SELECT CASE (run.ins).opc

          WHEN 'jmp' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).reg1),
                              run.regs), true) :: lifted_args
        END)
          UNION ALL
        SELECT 5, (
        SELECT CASE (run.ins).opc

          WHEN 'add' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || run.regs[(run.ins).reg2] + run.regs[(run.ins).reg3] || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 6, (
        SELECT CASE (run.ins).opc

          WHEN 'mul' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || run.regs[(run.ins).reg2] * run.regs[(run.ins).reg3] || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 7, (
        SELECT CASE (run.ins).opc

          WHEN 'div' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || run.regs[(run.ins).reg2] / run.regs[(run.ins).reg3] || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 8, (
        SELECT CASE (run.ins).opc

          WHEN 'mod' THEN (row((SELECT p FROM program AS p WHERE p.loc = (run.ins).loc+1),
                              run.regs[:(run.ins).reg1-1] || run.regs[(run.ins).reg2] % run.regs[(run.ins).reg3] || run.regs[(run.ins).reg1+1:]), true) :: lifted_args
        END)
      ),
      calls(site, fanout, ins, regs, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.ins, (s.out).args.regs, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, run.ins, run.regs, (
        SELECT CASE (run.ins).opc
          WHEN 'hlt' THEN run.regs[(run.ins).reg1]
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS calls(site, fanout, ins, regs, val)

      UNION ALL

    SELECT g.out_ins, g.out_regs, calls.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g, LATERAL (
      WITH
      memo(call_site, fanout, ins, regs, val) AS (
        SELECT NULL :: int, 0, g.out_ins, g.out_regs, memo.val
        FROM memoization_vm AS memo
        WHERE (g.out_ins, g.out_regs) = (memo.in_ins, memo.in_regs)
      ),
      slices(site, out) AS (
        SELECT 1, (
        SELECT CASE (g.out_ins).opc
          WHEN 'lod' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || (g.out_ins).reg2 || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 2, (
        SELECT CASE (g.out_ins).opc
          WHEN 'mov' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || g.out_regs[(g.out_ins).reg2] || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 3, (
        SELECT CASE (g.out_ins).opc
          WHEN 'jeq' THEN (row((SELECT p FROM program AS p WHERE p.loc = CASE WHEN g.out_regs[(g.out_ins).reg1] = g.out_regs[(g.out_ins).reg2]
                                                                             THEN (g.out_ins).reg3
                                                                             ELSE (g.out_ins).loc + 1
                                                                        END),
                              g.out_regs), true) :: lifted_args
        END)
          UNION ALL
        SELECT 4, (
        SELECT CASE (g.out_ins).opc

          WHEN 'jmp' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).reg1),
                              g.out_regs), true) :: lifted_args
        END)
          UNION ALL
        SELECT 5, (
        SELECT CASE (g.out_ins).opc

          WHEN 'add' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || g.out_regs[(g.out_ins).reg2] + g.out_regs[(g.out_ins).reg3] || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 6, (
        SELECT CASE (g.out_ins).opc

          WHEN 'mul' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || g.out_regs[(g.out_ins).reg2] * g.out_regs[(g.out_ins).reg3] || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 7, (
        SELECT CASE (g.out_ins).opc

          WHEN 'div' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || g.out_regs[(g.out_ins).reg2] / g.out_regs[(g.out_ins).reg3] || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
          UNION ALL
        SELECT 8, (
        SELECT CASE (g.out_ins).opc

          WHEN 'mod' THEN (row((SELECT p FROM program AS p WHERE p.loc = (g.out_ins).loc+1),
                              g.out_regs[:(g.out_ins).reg1-1] || g.out_regs[(g.out_ins).reg2] % g.out_regs[(g.out_ins).reg3] || g.out_regs[(g.out_ins).reg1+1:]), true) :: lifted_args
        END)
      ),
      calls(site, fanout, ins, regs, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.ins, (s.out).args.regs, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_ins, g.out_regs, (
        SELECT CASE (g.out_ins).opc
          WHEN 'hlt' THEN g.out_regs[(g.out_ins).reg1]
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS calls(site, fanout, ins, regs, val)
  ),
  base_cases(in_ins, in_regs, val) AS (
    SELECT g.in_ins, g.in_regs, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  memo AS (
    INSERT INTO memoization_vm ( SELECT DISTINCT run.ins, run.regs, b.val FROM base_cases AS b ) ON CONFLICT DO NOTHING
  )
  SELECT b.val
  FROM base_cases AS b;

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Compute the length of the Collatz sequence (also known as
-- the "3N + 1 problem") for the value N held in register R1.
--
\set N 42

SELECT :N,
       run((SELECT p FROM program AS p WHERE p.loc = 0), -- program entry instruction
           array[:N,0,0,0,0,0,0]) AS collatz             -- initial register file
