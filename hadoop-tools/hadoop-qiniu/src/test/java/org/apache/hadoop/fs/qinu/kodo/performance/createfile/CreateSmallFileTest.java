package org.apache.hadoop.fs.qinu.kodo.performance.createfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSmallFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSmallFileTest.class);

    private static long createLargeAmountSmall(String workDir, FileSystem fs, int files) throws Exception {
        String dir = workDir + "/testCreateLargeAmountSmallFiles/";
        fs.mkdirs(new Path(dir));

        long ms = System.currentTimeMillis();

        for (int i = 0; i < files; i++) {
            fs.create(new Path(dir + i)).close();
        }
        return System.currentTimeMillis() - ms;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs) throws Exception {
        return createLargeAmountSmall(testDir, fs, 10);
    }

}
