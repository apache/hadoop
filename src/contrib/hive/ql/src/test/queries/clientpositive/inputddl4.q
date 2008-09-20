-- a simple test to test sorted/clustered syntax
CREATE TABLE INPUTDDL4(viewTime DATETIME, userid INT,
                       page_url STRING, referrer_url STRING, 
                       friends ARRAY<BIGINT>, properties MAP<STRING, STRING>,
                       ip STRING COMMENT 'IP Address of the User') 
    COMMENT 'This is the page view table' 
    PARTITIONED BY(ds DATETIME, country STRING) 
    CLUSTERED BY(userid) SORTED BY(viewTime) INTO 32 BUCKETS;
DESCRIBE INPUTDDL4;
DROP TABLE INPUTDDL4;
