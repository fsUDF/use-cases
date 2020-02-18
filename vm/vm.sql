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
DROP FUNCTION IF EXISTS run(instruction, int[]);
CREATE FUNCTION run(ins instruction, regs int[]) RETURNS int AS
$$
  SELECT CASE ins.opc
    WHEN 'lod' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || ins.reg2 || regs[ins.reg1+1:])

    WHEN 'mov' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || regs[ins.reg2] || regs[ins.reg1+1:])

    WHEN 'jeq' THEN run((SELECT p FROM program AS p WHERE p.loc = CASE WHEN regs[ins.reg1] = regs[ins.reg2]
                                                                       THEN ins.reg3
                                                                       ELSE ins.loc + 1
                                                                  END),
                        regs)

    WHEN 'jmp' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.reg1),
                        regs)

    WHEN 'add' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || regs[ins.reg2] + regs[ins.reg3] || regs[ins.reg1+1:])

    WHEN 'mul' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || regs[ins.reg2] * regs[ins.reg3] || regs[ins.reg1+1:])

    WHEN 'div' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || regs[ins.reg2] / regs[ins.reg3] || regs[ins.reg1+1:])

    WHEN 'mod' THEN run((SELECT p FROM program AS p WHERE p.loc = ins.loc+1),
                        regs[:ins.reg1-1] || regs[ins.reg2] % regs[ins.reg3] || regs[ins.reg1+1:])

    WHEN 'hlt' THEN regs[ins.reg1]
  END
$$
LANGUAGE SQL STABLE STRICT;

-----------------------------------------------------------------------
-- Compute the length of the Collatz sequence (also known as
-- the "3N + 1 problem") for the value N held in register R1.
--
\set N 42

SELECT :N,
       run((SELECT p FROM program AS p WHERE p.loc = 0), -- program entry instruction
           array[:N,0,0,0,0,0,0]) AS collatz             -- initial register file
