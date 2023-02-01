package org.apache.hadoop.fs.qinu.kodo.performance.createfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSmallFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSmallFileTest.class);

    private static long createLargeAmountSmall(String workDir, FileSystem fs) throws Exception {
        String dir = workDir + "/testCreateLargeAmountSmallFiles/";
        fs.mkdirs(new Path(dir));

        long ms = System.currentTimeMillis();
        
        for (int i = 0; i < 100; i++) {
            fs.create(new Path(dir + i)).close();
        }
        return System.currentTimeMillis() - ms;
    }

    @Test
    public void testS3A() throws Exception {
        long time = createLargeAmountSmall(s3aTestDir, s3a);
        LOG.info("time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = createLargeAmountSmall(kodoTestDir, kodo);
        LOG.info("time: " + time);
    }
}
