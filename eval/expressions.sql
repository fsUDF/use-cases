-- Make the expression rowtype hashable and comparable
--
DROP FUNCTION IF EXISTS expression_hash(expression) CASCADE;
CREATE FUNCTION expression_hash(e expression) RETURNS int AS
$$
  SELECT e.node;
$$ LANGUAGE SQL IMMUTABLE;

DROP OPERATOR IF EXISTS =(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_eq(expression, expression) CASCADE;
CREATE FUNCTION expression_eq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) IS NOT DISTINCT FROM (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$ LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR =(leftarg = expression, rightarg = expression, procedure = expression_eq, commutator = =, HASHES, MERGES);

DROP OPERATOR IF EXISTS <=(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_leq(expression, expression) CASCADE;
CREATE FUNCTION expression_leq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) <= (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <=(leftarg = expression, rightarg = expression, procedure = expression_leq, commutator = >=);

DROP OPERATOR IF EXISTS <(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_lt(expression, expression) CASCADE;
CREATE FUNCTION expression_lt(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) < (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <(leftarg = expression, rightarg = expression, procedure = expression_lt, commutator = >);

DROP OPERATOR IF EXISTS >=(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_geq(expression, expression) CASCADE;
CREATE FUNCTION expression_geq(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) >= (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >=(leftarg = expression, rightarg = expression, procedure = expression_geq, commutator = <=);

DROP OPERATOR IF EXISTS >(expression, expression) CASCADE;
DROP FUNCTION IF EXISTS expression_gt(expression, expression) CASCADE;
CREATE FUNCTION expression_gt(e1 expression, e2 expression) RETURNS boolean AS
$$
  SELECT (e1.node, e1.op, e1.arg1, e1.arg2, e1.lit) > (e2.node, e2.op, e2.arg1, e2.arg2, e2.lit);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >(leftarg = expression, rightarg = expression, procedure = expression_gt, commutator = <);

DROP FUNCTION IF EXISTS expression_cmp(box, box) CASCADE;
CREATE FUNCTION expression_cmp(e1 expression, e2 expression) RETURNS int AS
$$
  SELECT CASE WHEN e1 = e2 THEN 0
              WHEN e1 < e2 THEN -1
              ELSE  1
         END;
$$
LANGUAGE SQL IMMUTABLE;

DROP OPERATOR CLASS IF EXISTS expression_hash_ops USING hash CASCADE;
CREATE OPERATOR CLASS expression_hash_ops DEFAULT FOR TYPE expression USING hash AS
  operator 1 =,
  function 1 expression_hash(expression);

DROP OPERATOR CLASS IF EXISTS expression_btree_ops USING btree CASCADE;
CREATE OPERATOR CLASS expression_btree_ops DEFAULT FOR TYPE expression USING btree AS
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 >,
  function 1 expression_cmp(expression, expression);
