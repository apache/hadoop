CREATE TABLE dest1(c1 STRING);

FROM src INSERT OVERWRITE TABLE dest1 SELECT '  abc  ' WHERE src.key = 86;

EXPLAIN
SELECT round(1.0), round(1.5), round(-1.5), floor(1.0), floor(1.5), floor(-1.5), ceil(1.0), ceil(1.5), ceil(-1.5), ceiling(1.0), rand(3), +3, -3, 1++2, 1+-2, ~1 FROM dest1;

SELECT round(1.0), round(1.5), round(-1.5), floor(1.0), floor(1.5), floor(-1.5), ceil(1.0), ceil(1.5), ceil(-1.5), ceiling(1.0), rand(3), +3, -3, 1++2, 1+-2, ~1 FROM dest1;
