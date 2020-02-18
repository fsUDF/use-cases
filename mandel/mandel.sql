-- Render an ASCII art approximation of the Mandelbrot Set
--
-- This has been directly adapted from SQLite's "Outlandish Recursive Query Examples"
-- found at https://www.sqlite.org/lang_with.html


-- Tail-recursive functional SQL UDF to approximate the Mandelbrot Set
-- at point (cx, cy)
--
DROP FUNCTION IF EXISTS m(int, float, float, float, float);
CREATE FUNCTION m(iter int, cx float, cy float, x float, y float) RETURNS int AS
$$
  SELECT CASE WHEN NOT (x^2 + y^2 < 4.0 AND iter < 28)
              THEN iter
              ELSE m(iter + 1,
                     cx,
                     cy,
                     x^2 - y^2   + cx,
                     2.0 * x * y + cy)
         END;
$$
LANGUAGE SQL IMMUTABLE STRICT;

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
