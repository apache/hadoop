package org.apache.hadoop.fs.qinu.kodo.performance.mkdir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MkdirDeeplyTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(MkdirDeeplyTest.class);

    private long testMkdirDeeply(String workDir, FileSystem fs, int dirs, int deep) throws Exception {
        String dir = workDir + "/testMkdirDeeply";
        long ms = System.currentTimeMillis();
        for (int i = 0; i < dirs; i++) {
            StringBuilder d1 = new StringBuilder(dir + "/" + i);
            for (int j = 0; j < deep; j++) {
                d1.append("/").append(j);
            }
            fs.mkdirs(new Path(d1.toString()));
        }
        return System.currentTimeMillis() - ms;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs) throws Exception {
        return testMkdirDeeply(testDir, fs, 10, 10);
    }
}
