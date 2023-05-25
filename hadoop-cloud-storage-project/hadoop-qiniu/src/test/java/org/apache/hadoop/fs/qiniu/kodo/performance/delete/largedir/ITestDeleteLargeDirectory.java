package org.apache.hadoop.fs.qiniu.kodo.performance.delete.largedir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestDeleteLargeDirectory extends QiniuKodoPerformanceBaseTest {
    protected int files() {
        return 2000;
    }

    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        getPrepareHelper().prepareLargeDir(testDir, files());
        service.submit(() -> {
            try {
                fs.delete(new Path(testDir));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        long ms = System.currentTimeMillis();
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
