FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.value WHERE key < 100 group by src.key
