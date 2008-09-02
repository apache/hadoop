FROM src
INSERT OVERWRITE TABLE dest1 SELECT concat('1234', 'abc', 'extra argument'), src.value WHERE src.key < 100
