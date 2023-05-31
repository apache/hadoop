package org.apache.hadoop.fs.qiniu.kodo.performance.delete.lotsfiles;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class ADeleteLotsOfFilesTest extends QiniuKodoPerformanceBaseTest {
    public int files() {
        return 1000;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        getPrepareHelper().prepareLargeDir(testDir, files());

        for (int i = 0; i < files(); i++) {
            Path p = new Path(testDir + "/" + i);
            service.submit(() -> {
                try {
                    fs.delete(p, false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        long ms = System.currentTimeMillis();
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
