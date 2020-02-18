# A Collection of Recursive SQL UDFs in Functional Style

These 10 subdirectories contain the SQL-based implementations of
a variety of algorithmic problems.  Please refer to Table 2 in
Section 5 of the accompanying paper.  The SQL code should be
ready for execution on PostgreSQL (tested with
version 11.3).

In each subdirectory, we provide two versions of the UDFs:

1. The original, non-compiled version of the UDF, expressed in the recursive
   functional style (file name `‹fun›.sql`).

2. The compiled version of the UDF (file name `‹fun›-compiled.sql`).

Both versions can be run from the command line as follows:

~~~
$ cd ‹fun›
$ psql -d ‹database› -f ‹fun›.sql
$ psql -d ‹database› -f ‹fun›-compiled.sql
~~~

These SQL scripts are self-contained and create their own test
data when you invoke them.  Original and compiled versions
of the UDFs operate on the same data, using the same function
arguments.

**Note:** 
We have compiled the tail-recursive functions in subdirectories

- `mandel`
- `paths`
- `sizes`
- `vm`

using the template for tail recursion (see Figure 26 in the paper) but
use `WITH RECURSIVE` in these files so that they run a vanilla PostgreSQL
instance.  Using `WITH ITERATE` further improves the time and
space efficiency of these functions.
