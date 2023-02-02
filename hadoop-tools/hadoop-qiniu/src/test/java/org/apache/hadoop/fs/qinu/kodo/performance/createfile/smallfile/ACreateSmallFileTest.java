package org.apache.hadoop.fs.qinu.kodo.performance.createfile.smallfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ACreateSmallFileTest extends QiniuKodoPerformanceBaseTest {
    protected int files = 100;

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService executorService) throws Exception {
        long ms = System.currentTimeMillis();

        for (int i = 0; i < files; i++) {
            final Path p = new Path(testDir + "/" + i);
            executorService.submit(() -> {
                try {
                    fs.create(p).close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            throw new TimeoutException("More than 1 minute");
        }
        return System.currentTimeMillis() - ms;
    }
}
