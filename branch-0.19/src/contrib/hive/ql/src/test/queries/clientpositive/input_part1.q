CREATE TABLE dest1(key INT, value STRING, hr STRING, ds STRING);

EXPLAIN EXTENDED
FROM srcpart
INSERT OVERWRITE TABLE dest1 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12';

FROM srcpart
INSERT OVERWRITE TABLE dest1 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12';

SELECT dest1.* FROM dest1;

