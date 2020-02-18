-- Longest common subsequence
--
-- Consider sequences (ABCD) and (ACBAD).
-- They have 5 length-2 common subsequences: (AB), (AC), (AD), (BD), and (CD);
-- 2 length-3 common subsequences: (ABD) and (ACD); and no longer common subsequences.
-- So (ABD) and (ACD) are their longest common subsequences.
--
-- Example source: https://en.wikipedia.org/wiki/Longest_common_subsequence_problem

\set min_length 8  -- Minimum length of a RNA sequence
\set max_length 7  -- Maximum length of a RNA sequence
\set sequences 100 -- How many RNA sequences we will have

SELECT setseed(0.42);

-- An RNA sequence is a sequence of either guanine, uracil, adenine and cytosine each
-- denoted by G, U, A, and C respectively.
DROP TABLE IF EXISTS sequences;
CREATE TABLE sequences (id int, seq text);

-- Generate random RNA sequences.
INSERT INTO sequences
WITH RECURSIVE
sequences(id, s) AS (
  SELECT 1, seq
  FROM (SELECT STRING_AGG((array['G','U','A','C'])[1 + random()*(4-1)],'')
        FROM generate_series(1,floor(:min_length + random() * (:max_length - :min_length + 1)) :: int)) AS _(seq)
    UNION ALL
  SELECT s.id + 1, seq
  FROM   sequences AS s, (SELECT STRING_AGG((array['G','U','A','C'])[1 + random()*(4-1)],'')
                          FROM   (SELECT :min_length + random() * (:max_length - :min_length + 1)) AS _(r), generate_series(1,floor(r) :: int)) AS _(seq)
  WHERE  s.id < :sequences
)
TABLE sequences;

-- Find the length of the longest common subsequence.

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_lcs (in_l text, in_r text, val int, PRIMARY KEY(in_l, in_r));

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
CREATE TYPE args AS (l text, r text);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS lcs(text, text);
CREATE OR REPLACE FUNCTION lcs(l text, r text)
RETURNS int AS $$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_l, in_r, site, fanout, out_l, out_r, val) AS (
    SELECT lcs.l, lcs.r, edges.*
    FROM (
      WITH
      memo(call_site, fanout, l, r, val) AS (
        SELECT NULL :: int, 0, lcs.l, lcs.r, memo.val
        FROM memoization_lcs AS memo
        WHERE (lcs.l, lcs.r) = (memo.in_l, memo.in_r)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN lcs.l = '' OR lcs.r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(lcs.l,1) = left(lcs.r,1) THEN ((right(lcs.l,-1), right(lcs.r,-1)), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN lcs.l = '' OR lcs.r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(lcs.l,1) = left(lcs.r,1) THEN (NULL, false) :: lifted_args
            ELSE ((right(lcs.l,-1), lcs.r), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN lcs.l = '' OR lcs.r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(lcs.l,1) = left(lcs.r,1) THEN (NULL, false) :: lifted_args
            ELSE ((lcs.l, right(lcs.r,-1)), true) :: lifted_args
          END
        )
      ),
      calls(site, fanout, l, r, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.l, (s.out).args.r, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, lcs.l, lcs.r, (
        SELECT CASE
          WHEN lcs.l = '' OR lcs.r = '' THEN 0
          WHEN left(lcs.l,1) = left(lcs.r,1) THEN 1 + NULL :: int
          ELSE GREATEST(NULL :: int, NULL :: int)
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, l, r, val)

      UNION

    SELECT g.out_l, g.out_r, edges.*
    FROM call_graph AS g,
      LATERAL (
      WITH
      memo(call_site, fanout, l, r, val) AS (
        SELECT NULL :: int, 0, g.out_l, g.out_r, memo.val
        FROM memoization_lcs AS memo
        WHERE (g.out_l, g.out_r) = (memo.in_l, memo.in_r)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN g.out_l = '' OR g.out_r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(g.out_l,1) = left(g.out_r,1) THEN ((right(g.out_l,-1), right(g.out_r,-1)), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 2, (
          SELECT CASE
            WHEN g.out_l = '' OR g.out_r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(g.out_l,1) = left(g.out_r,1) THEN (NULL, false) :: lifted_args
            ELSE ((right(g.out_l,-1), g.out_r), true) :: lifted_args
          END
        )
          UNION ALL
        SELECT 3, (
          SELECT CASE
            WHEN g.out_l = '' OR g.out_r = ''      THEN (NULL, false) :: lifted_args
            WHEN left(g.out_l,1) = left(g.out_r,1) THEN (NULL, false) :: lifted_args
            ELSE ((g.out_l, right(g.out_r,-1)), true) :: lifted_args
          END
        )
      ),
      calls(site, fanout, l, r, val) AS (
        SELECT s.site, COUNT(*) OVER (), (s.out).args.l, (s.out).args.r, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_l, g.out_r, (
        SELECT CASE
          WHEN g.out_l = '' OR g.out_r = '' THEN 0
          WHEN left(g.out_l,1) = left(g.out_r,1) THEN 1 + NULL :: int
          ELSE GREATEST(NULL :: int, NULL :: int)
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, l, r, val)
  ),
  base_cases(in_l, in_r, val) AS (
    SELECT g.in_l, g.in_r, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_l, in_r, val) AS (
    TABLE base_cases
      UNION ALL (
    WITH e AS (TABLE evaluation),
    returns(in_l, in_r, val) AS (
      SELECT go.in_l, go.in_r, (
        SELECT CASE
          WHEN go.in_l = '' OR go.in_r = '' THEN 0
          WHEN left(go.in_l,1) = left(go.in_r,1) THEN 1 + go.ret[1]
          ELSE GREATEST(go.ret[2], go.ret[3])
        END
      )
      FROM (
        SELECT   g.in_l, g.in_r, array_gather(g.e_val, g.site) AS ret
        FROM     (SELECT g.*, e.val AS e_val FROM call_graph AS g, e WHERE (g.out_l, g.out_r) = (e.in_l, e.in_r) OFFSET 0) AS g
        WHERE    NOT EXISTS (SELECT 1 FROM e WHERE (e.in_l, e.in_r) = (g.in_l, g.in_r))
        GROUP BY g.in_l, g.in_r, g.fanout
        HAVING   COUNT(*) = g.fanout
      ) AS go(in_l, in_r, ret)
    )
    SELECT next.*
    FROM ( TABLE e UNION ALL TABLE returns ) AS next(in_l, in_r, val)
    WHERE NOT EXISTS (SELECT 1 FROM e WHERE (e.in_l, e.in_r) = (lcs.l, lcs.r))
  )),
  memo AS (
    INSERT INTO memoization_lcs ( SELECT DISTINCT e.in_l, e.in_r, e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_l, e.in_r) = (lcs.l, lcs.r);

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-----------------------------------------------------------------------
-- Determine the length of the longest common subsequence of seq1 and seq2
\set seq1 2
\set seq2 3

SELECT s1.seq, s2.seq, lcs(s1.seq,s2.seq)
FROM   sequences AS s1, sequences AS s2
WHERE  s1.id = :seq1
AND    s2.id = :seq2;
