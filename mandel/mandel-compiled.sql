-- Render an ASCII art approximation of the Mandelbrot Set
--
-- This has been directly adapted from SQLite's "Outlandish Recursive Query Examples"
-- found at https://www.sqlite.org/lang_with.html


-- Tail-recursive functional SQL UDF to approximate the Mandelbrot Set
-- at point (cx, cy)
--

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_mandel (in_iter int, in_cx float, in_cy float, in_x float, in_y float, val int, PRIMARY KEY(in_iter, in_cx, in_cy, in_x, in_y));

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
CREATE TYPE args AS (iter int, x float, y float);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS m(int, float, float, float, float);
CREATE FUNCTION m(iter int, cx float, cy float, x float, y float) RETURNS int AS
$$

--  BEGIN GENERATED CODE

    --  ITERATE
  WITH RECURSIVE call_graph(in_iter, in_x, in_y, site, fanout, out_iter, out_x, out_y, val) AS (
    SELECT m.iter, m.x, m.y, edges.*
    FROM (
      WITH
      memo(site, fanout, iter, x, y, val) AS (
        SELECT NULL :: int, 0, m.iter, m.x, m.y, memo.val
        FROM memoization_mandel AS memo
        WHERE (m.iter, m.cx, m.cy, m.x, m.y) = (memo.in_iter, memo.in_cx, memo.in_cy, memo.in_x, memo.in_y)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE WHEN NOT (m.x^2 + m.y^2 < 4.0 AND m.iter < 28)
                      THEN (NULL, false) :: lifted_args
                      ELSE (row(m.iter + 1,
                             m.x^2 - m.y^2   + m.cx,
                             2.0 * m.x * m.y + m.cy), true) :: lifted_args
          END)
      ),
      calls(site, fanout, iter, x, y, val) AS (
        SELECT s.site, 1, (s.out).args.iter, (s.out).args.x, (s.out).args.y, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, m.iter, m.x, m.y, (
         SELECT CASE WHEN NOT (m.x^2 + m.y^2 < 4.0 AND m.iter < 28)
              THEN m.iter
              ELSE NULL :: int
         END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, iter, x, y, val)

      UNION ALL

    SELECT g.out_iter, g.out_x, g.out_y, edges.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g, LATERAL (
      WITH
      memo(site, fanout, iter, x, y, val) AS (
        SELECT NULL :: int, 0, g.out_iter, g.out_x, g.out_y, memo.val
        FROM memoization_mandel AS memo
        WHERE (g.out_iter, m.cx, m.cy, g.out_x, g.out_y) = (memo.in_iter, memo.in_cx, memo.in_cy, memo.in_x, memo.in_y)
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE WHEN NOT (g.out_x^2 + g.out_y^2 < 4.0 AND g.out_iter < 28)
                      THEN (NULL, false) :: lifted_args
                      ELSE (row(g.out_iter + 1,
                             g.out_x^2 - g.out_y^2   + m.cx,
                             2.0 * g.out_x * g.out_y + m.cy), true) :: lifted_args
          END)
      ),
      calls(site, fanout, iter, x, y, val) AS (
        SELECT s.site, 1, (s.out).args.iter, (s.out).args.x, (s.out).args.y, NULL :: int
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_iter, g.out_x, g.out_y, (
         SELECT CASE WHEN NOT (g.out_x^2 + g.out_y^2 < 4.0 AND g.out_iter < 28)
              THEN g.out_iter
              ELSE NULL :: int
         END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, iter, x, y, val)
  ),
  base_cases(iter, x, y, val) AS (
    SELECT g.in_iter, g.in_x, g.in_y, g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  memo AS (
    INSERT INTO memoization_mandel ( SELECT DISTINCT m.iter, m.cx, m.cy, m.x, m.y, b.val FROM base_cases AS b ) ON CONFLICT DO NOTHING
  )
  SELECT b.val
  FROM base_cases AS b;

--  END GENERATED CODE

$$ LANGUAGE SQL;

-- Resolution in pixels on the y axis (N ⩾ 5 for sensible results)
\set N 20

-- Define regions on x/y axes and approximate the Mandelbrot Set in
-- the resulting x/y area
--
WITH
xaxis(x) AS (
  SELECT i AS x
  FROM   generate_series(-2.0, 1.2, (1.2 - (-2.0)) / (3 * :N)) AS i
),
yaxis(y) AS (
  SELECT i AS y
  FROM   generate_series(-1.0, 1.0, (1.0 - (-1.0)) / :N) AS i
),
m2(iter, cx, cy) AS (
  SELECT m(0, x, y, 0.0, 0.0) AS iter, x AS cx, y AS cy
  FROM   xaxis AS x, yaxis AS y
),
-- Render the result by approximating the function results
-- using ASCII characters of increasing density of "ink"
--
a(cy, t) AS (
  SELECT cy, string_agg(substr(' .+*#', 1 + LEAST(iter / 7, 4), 1), NULL ORDER BY cx) AS t
  FROM   m2
  GROUP BY cy
)
SELECT t
FROM   a
ORDER BY cy;
