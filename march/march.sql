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

DROP FUNCTION IF EXISTS march(int[], int[], bool);
CREATE FUNCTION march(current int[], goal int[], "track?" bool) RETURNS int[][] AS
$$
  SELECT CASE
    WHEN "track?" AND current = goal THEN array[] :: int[][]
    ELSE CASE WHEN "track?"
              THEN array[current]
              ELSE array[] :: int[][]
         END
         ||
         (SELECT march(array[current[1] + d.dir[0], current[2] + d.dir[1]] :: int[],
                       CASE WHEN d."track?"
                            THEN goal
                            ELSE array[current[1] + d.dir[0], current[2] + d.dir[1]] :: int[]
                       END,
                       d."track?")
          FROM   squares AS s, directions AS d
          WHERE  current = s.xy
          AND    (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur)
         )
  END;
$$
LANGUAGE SQL STRICT;

-- Trace the shape's border in the 2D map, starting from (x,y)
--
\set x 5
\set y 12

SELECT point(:x, :y) AS start, march(array[:x, :y], array[:x, :y], false) AS border;
