package org.apache.hadoop;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogGeneration {
    public static final Logger LOG = LoggerFactory.getLogger(TestLogGeneration.class);


    @Test
    public void testExample() {
        LOG.info("This is an info log message.");
        LOG.debug("This is a debug log message.");
        LOG.error("This is an error log message.");
        assert true;
    }
}
