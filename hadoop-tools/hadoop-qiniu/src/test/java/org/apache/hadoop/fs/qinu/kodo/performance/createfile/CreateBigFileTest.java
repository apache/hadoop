package org.apache.hadoop.fs.qinu.kodo.performance.createfile;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBigFileTest extends QiniuKodoPerformanceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreateBigFileTest.class);

    private long createBigFile(String workDir, FileSystem fs, int blockSize, int blocks) throws Exception {
        byte[] bs = new byte[blockSize];
        final String dir = workDir + "/testCreateSingleBigFile/";
        fs.mkdirs(new Path(dir));

        long ms = System.currentTimeMillis();

        FSDataOutputStream fos = fs.create(new Path(dir + "/bigFile"));
        for (int i = 0; i < blocks; i++) {
            fos.write(bs);
        }
        fos.close();

        return System.currentTimeMillis() - ms;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs) throws Exception {
        return createBigFile(testDir, fs, 4 * 1024 * 1024, 2);
    }

}
