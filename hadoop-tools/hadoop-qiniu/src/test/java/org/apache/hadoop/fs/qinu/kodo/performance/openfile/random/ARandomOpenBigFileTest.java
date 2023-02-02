package org.apache.hadoop.fs.qinu.kodo.performance.openfile.random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.apache.hadoop.fs.qinu.kodo.performance.openfile.OpenBigFileCommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class ARandomOpenBigFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ARandomOpenBigFileTest.class);
    protected int blockSize = 4 * 1024 * 1024;
    protected int blocks = 10;

    protected int readers = 3;
    protected int randomReadCount = 100;

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        Path p = OpenBigFileCommonUtil.makeSureExistsBigFile(testDir, fs, blockSize, blocks);
        long ms = System.currentTimeMillis();

        for (int i = 0; i < readers; i++) {
            service.submit(() -> {
                try {
                    FSDataInputStream fis = fs.open(p);
                    int len = fis.available();

                    for (int j = 0; j < randomReadCount; j++) {
                        int pos = (int) (Math.random() * len);
                        fis.seek(pos);
                        int b = fis.read();
                    }

                    fis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            });
        }
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
