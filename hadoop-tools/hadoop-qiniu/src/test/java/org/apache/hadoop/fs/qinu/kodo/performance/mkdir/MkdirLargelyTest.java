package org.apache.hadoop.fs.qinu.kodo.performance.mkdir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MkdirLargelyTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(MkdirLargelyTest.class);

    private long testMkdirLargely(String workDir, FileSystem fs, int dirs) throws Exception {
        final String dir = workDir + "/testMkdirLargely/";

        long ms = System.currentTimeMillis();
        for (int i = 0; i < dirs; i++) {
            fs.mkdirs(new Path(dir + "/" + i));
        }
        return System.currentTimeMillis() - ms;
    }

    @Test
    public void testS3A() throws Exception {
        long time = testMkdirLargely(s3aTestDir, s3a, 100);
        LOG.info("time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = testMkdirLargely(kodoTestDir, kodo, 100);
        LOG.info("time: " + time);
    }
}
