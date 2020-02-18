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
CREATE OR REPLACE FUNCTION lcs(l text, r text)
RETURNS int AS $$
  SELECT CASE
    WHEN l = '' OR r = '' THEN 0
    WHEN left(l,1) = left(r,1) THEN 1 + lcs(right(l,-1), right(r,-1))
    ELSE GREATEST(lcs(right(l,-1), r), lcs(l, right(r,-1)))
  END;
$$ LANGUAGE SQL STABLE STRICT;

-----------------------------------------------------------------------
-- Determine the length of the longest common subsequence of seq1 and seq2
\set seq1 2
\set seq2 3

SELECT s1.seq, s2.seq, lcs(s1.seq,s2.seq)
FROM   sequences AS s1, sequences AS s2
WHERE  s1.id = :seq1
AND    s2.id = :seq2;
