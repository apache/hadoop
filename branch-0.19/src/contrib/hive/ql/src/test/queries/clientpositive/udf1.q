CREATE TABLE dest1(c1 STRING, c2 STRING, c3 STRING, c4 STRING,
  c5 STRING, c6 STRING, c7 STRING, c8 STRING,
  c9 STRING, c10 STRING, c11 STRING, c12 STRING, c13 STRING,
  c14 STRING, c15 STRING, c16 STRING, c17 STRING);

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT 'a' LIKE '%a%', 'b' LIKE '%a%', 'ab' LIKE '%a%', 'ab' LIKE '%a_',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a',
  '' RLIKE '.*', 'a' RLIKE '[ab]', '' RLIKE '[ab]', 'hadoop' RLIKE '[a-z]*', 'hadoop' RLIKE 'o*',
  REGEXP_REPLACE('abc', 'b', 'c'), REGEXP_REPLACE('abc', 'z', 'a'), REGEXP_REPLACE('abbbb', 'bb', 'b'), REGEXP_REPLACE('hadoop', '(.)[a-z]*', '$1ive')
  WHERE src.key = 86;

FROM src INSERT OVERWRITE TABLE dest1 SELECT 'a' LIKE '%a%', 'b' LIKE '%a%', 'ab' LIKE '%a%', 'ab' LIKE '%a_',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a',
  '' RLIKE '.*', 'a' RLIKE '[ab]', '' RLIKE '[ab]', 'hadoop' RLIKE '[a-z]*', 'hadoop' RLIKE 'o*',
  REGEXP_REPLACE('abc', 'b', 'c'), REGEXP_REPLACE('abc', 'z', 'a'), REGEXP_REPLACE('abbbb', 'bb', 'b'), REGEXP_REPLACE('hadoop', '(.)[a-z]*', '$1ive')
  WHERE src.key = 86;

SELECT dest1.* FROM dest1;
