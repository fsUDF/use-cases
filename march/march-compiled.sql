-- The Marching Squares algorithm.
--
-- Recursive SQL UDF in functional style that wanders the 2D map
-- to detect and track the border of a 2D shape. Returns an array
-- describing a closed path around the shape.
--

-- Marching Squares to trace an isoline (contour line) on a height map
--
-- See https://en.wikipedia.org/wiki/Marching_squares.

-- Custom operations on PostgreSQL's types point and box
--
-- • equality, comparisons
-- • B+tree ops for types point and box
-- • scalar multiplication & division (from left and right)
-- • AVG(point), SUM(point)

-- Define aggregates, operations, comparisons on PostgreSQL points
--
\i points.sql

-- Representation of 2D height map
--
\i map.sql

-- Tabular encoding of the Essence of the marching square algorithm:
-- direct the march based on a 2×2 vicinity of pixels
--
DROP TABLE IF EXISTS directions CASCADE;
CREATE TABLE directions (
  ll       bool,   -- pixel set in the lower left?
  lr       bool,   --                  lower right
  ul       bool,   --                  upper left
  ur       bool,   --                  upper right
  dir      point,  -- direction of march
  "track?" bool,   -- are we tracking the shape yet?
  PRIMARY KEY (ll, lr, ul, ur));

INSERT INTO directions(ll, lr, ul, ur, dir, "track?") VALUES
  (false,false,false,false, point( 1, 0), false), -- | | ︎: →
  (false,false,false,true , point( 1, 0), true ), -- |▝| : →
  (false,false,true ,false, point( 0, 1), true ), -- |▘| : ↑
  (false,false,true ,true , point( 1, 0), true ), -- |▀| : →
  (false,true ,false,false, point( 0,-1), true ), -- |▗| : ↓
  (false,true ,false,true , point( 0,-1), true ), -- |▐| : ↓
  (false,true ,true ,false, point( 0, 1), true ), -- |▚| : ↑
  (false,true ,true ,true , point( 0,-1), true ), -- |▜| : ↓
  (true ,false,false,false, point(-1, 0), true ), -- |▖| : ←
  (true ,false,false,true , point(-1, 0), true ), -- |▞| : ←
  (true ,false,true ,false, point( 0, 1), true ), -- |▌| : ↑
  (true ,false,true ,true , point( 1, 0), true ), -- |▛| : →
  (true ,true ,false,false, point(-1, 0), true ), -- |▄| : ←
  (true ,true ,false,true , point(-1, 0), true ), -- |▟| : ←
  (true ,true ,true ,false, point( 0, 1), true ), -- |▛| : →
  (true ,true ,true ,true , NULL        , true ); -- |█| : x


-- Generate a thresholded black/white 2D map of pixels
--
DROP TABLE IF EXISTS pixels CASCADE;
CREATE TABLE pixels (
  xy  point PRIMARY KEY,
  alt bool);

INSERT INTO pixels(xy, alt)
  -- Threshold height map based on given iso value (here: > 700)
  SELECT m.xy, m.alt > 700 AS alt
  FROM   map AS m;


-- Generate a 2D map of squares that each aggregate 2×2 adjacent pixel
--
DROP TABLE IF EXISTS squares CASCADE;
CREATE TABLE squares (
  xy int[] PRIMARY KEY,
  ll bool,
  lr bool,
  ul bool,
  ur bool);

INSERT INTO squares(xy, ll, lr, ul, ur)
  -- Establish 2×2 squares on the pixel-fied map,
  -- (x,y) designates lower-left corner: ul  ur
  --                                       ⬜︎
  --                                     ll  lr
  SELECT array[p0.xy[0], p0.xy[1]] AS xy,
         p0.alt AS ll, p1.alt AS lr, p2.alt AS ul, p3.alt AS ur
  FROM   pixels p0, pixels p1, pixels p2, pixels p3
  WHERE  p1.xy = point(p0.xy[0]+1, p0.xy[1])
  AND    p2.xy = point(p0.xy[0]  , p0.xy[1]+1)
  AND    p3.xy = point(p0.xy[0]+1, p0.xy[1]+1);

--  BEGIN INITIALIZATION

-- Memoization table
CREATE TABLE IF NOT EXISTS memoization_march (in_current int[], in_goal int[], "in_track?" bool, val int[][], PRIMARY KEY(in_current, in_goal, "in_track?"));

-- (lifted) argument and result types
DROP TYPE IF EXISTS lifted_args CASCADE;
DROP TYPE IF EXISTS args CASCADE;
CREATE TYPE args AS (current int[], goal int[], "track?" bool);
CREATE TYPE lifted_args AS (args args, not_bottom boolean);

--  END INITIALIZATION

DROP FUNCTION IF EXISTS march(int[], int[], bool);
CREATE FUNCTION march(current int[], goal int[], "track?" bool) RETURNS int[][] AS
$$

--  BEGIN GENERATED CODE

  WITH RECURSIVE
  call_graph(in_current, in_goal, "in_track?", site, fanout, out_current, out_goal, "out_track?") AS (
    SELECT march.current, march.goal, march."track?", edges.*
    FROM (
      WITH
      memo(site, fanout, current, goal, "track?", val) AS (
        SELECT NULL :: int, 0, march.current, march.goal, march."track?", memo.val
        FROM memoization_march AS memo
        WHERE (march.current, march.goal, march."track?") = (memo.in_current, memo.in_goal, memo."in_track?")
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN march."track?" AND march.current = march.goal THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((array[march.current[1] + d.dir[0], march.current[2] + d.dir[1]] :: int[],
                               CASE WHEN d."track?"
                                    THEN march.goal
                                    ELSE array[march.current[1] + d.dir[0], march.current[2] + d.dir[1]] :: int[]
                               END,
                               d."track?"), true) :: lifted_args
                  FROM   squares AS s, directions AS d
                  WHERE  march.current = s.xy
                  AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
                 )
          END
        )
      ),
      calls(site, fanout, current, goal, "track?", val) AS (
        SELECT s.site, 1, (s.out).args.current, (s.out).args.goal, (s.out).args."track?", NULL :: int[][]
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, march.current, march.goal, march."track?", (
        SELECT CASE
          WHEN march."track?" AND march.current = march.goal THEN array[] :: int[][]
          ELSE CASE WHEN march."track?"
                    THEN array[march.current]
                    ELSE array[] :: int[][]
               END
               ||
               (SELECT NULL :: int[][]
                FROM   squares AS s, directions AS d
                WHERE  march.current = s.xy
                AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
               )
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(site, fanout, current, goal, "track?", val)

      UNION ALL

    SELECT g.out_current, g.out_goal, g."out_track?", edges.*
    FROM (SELECT * FROM call_graph AS g WHERE g.fanout > 0) AS g,
      LATERAL (
      WITH
      memo(site, fanout, current, goal, "track?", val) AS (
        SELECT NULL :: int, 0, g.out_current, g.out_goal, g."out_track?", memo.val
        FROM memoization_march AS memo
        WHERE (g.out_current, g.out_goal, g."out_track?") = (memo.in_current, memo.in_goal, memo."in_track?")
      ),
      slices(site, out) AS (
        SELECT 1, (
          SELECT CASE
            WHEN g."out_track?" AND g.out_current = g.out_goal THEN (NULL, false) :: lifted_args
            ELSE (SELECT ((array[g.out_current[1] + d.dir[0], g.out_current[2] + d.dir[1]] :: int[],
                               CASE WHEN d."track?"
                                    THEN g.out_goal
                                    ELSE array[g.out_current[1] + d.dir[0], g.out_current[2] + d.dir[1]] :: int[]
                               END,
                               d."track?"), true) :: lifted_args
                  FROM   squares AS s, directions AS d
                  WHERE  g.out_current = s.xy
                  AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
                 )
          END
        )
      ),
      calls(site, fanout, current, goal, "track?", val) AS (
        SELECT s.site, 1, (s.out).args.current, (s.out).args.goal, (s.out).args."track?", NULL :: int[][]
        FROM   slices AS s
        WHERE  (s.out).not_bottom
      )
      TABLE memo
        UNION ALL
      SELECT *
      FROM   calls
      WHERE  NOT EXISTS (TABLE memo)
        UNION ALL
      SELECT NULL :: int, 0, g.out_current, g.out_goal, g."out_track?", (
        SELECT CASE
          WHEN g."out_track?" AND g.out_current = g.out_goal THEN array[] :: int[][]
          ELSE CASE WHEN g."out_track?"
                    THEN array[g.out_current]
                    ELSE array[] :: int[][]
               END
               ||
               (SELECT NULL :: int[][]
                FROM   squares AS s, directions AS d
                WHERE  g.out_current = s.xy
                AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
               )
        END
      )
      WHERE  NOT EXISTS (TABLE memo)
      AND    NOT EXISTS (TABLE calls)
    ) AS edges(fanout, current, goal, "track?", val)
  ),
  base_cases(in_current, in_goal, "in_track?", val) AS (
    SELECT g.in_current, g.in_goal, g."in_track?", g.val
    FROM   call_graph AS g
    WHERE  g.fanout = 0
  ),
  evaluation(in_current, in_goal, "in_track?", val) AS (
    TABLE base_cases
      UNION
    SELECT go.in_current, go.in_goal, go."in_track?", (
          SELECT CASE
            WHEN go."in_track?" AND go.in_current = go.in_goal THEN array[] :: int[][]
            ELSE CASE WHEN go."in_track?"
                      THEN array[go.in_current]
                      ELSE array[] :: int[][]
                 END
                 ||
                 (SELECT go.val
                  FROM   squares AS s, directions AS d
                  WHERE  go.in_current = s.xy
                  AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
                 )
          END
        )
    FROM (
      SELECT g.in_current, g.in_goal, g."in_track?", e.val
      FROM   call_graph AS g, evaluation AS e
      WHERE  (g.out_current, g.out_goal, g."out_track?") = (e.in_current, e.in_goal, e."in_track?")
    ) AS go(in_current, in_goal, "in_track?", val)
  ),
  memo AS (
    INSERT INTO memoization_march ( SELECT DISTINCT e.in_current, e.in_goal, e."in_track?", e.val FROM evaluation AS e ) ON CONFLICT DO NOTHING
  )
  SELECT e.val
  FROM   evaluation AS e
  WHERE  (e.in_current, e.in_goal, e."in_track?") = (march.current, march.goal, march."track?");

--  END GENERATED CODE

$$ LANGUAGE SQL VOLATILE STRICT;

-- Trace the shape's border in the 2D map, starting from (x,y)
--
\set x 5
\set y 12

SELECT point(:x, :y) AS start, march(array[:x, :y], array[:x, :y], false) AS border;
