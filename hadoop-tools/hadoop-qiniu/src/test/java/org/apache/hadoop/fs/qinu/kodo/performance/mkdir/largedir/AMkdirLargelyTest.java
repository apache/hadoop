package org.apache.hadoop.fs.qinu.kodo.performance.mkdir.largedir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class AMkdirLargelyTest extends QiniuKodoPerformanceBaseTest {
    abstract protected int dirs();

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        long ms = System.currentTimeMillis();

        for (int i = 0; i < dirs(); i++) {
            final Path p = new Path(testDir + "/" + i);
            service.submit(() -> {
                try {
                    fs.mkdirs(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
