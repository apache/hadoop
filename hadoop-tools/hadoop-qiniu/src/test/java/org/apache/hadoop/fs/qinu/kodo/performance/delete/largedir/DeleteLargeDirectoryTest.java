package org.apache.hadoop.fs.qinu.kodo.performance.delete.largedir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DeleteLargeDirectoryTest extends QiniuKodoPerformanceBaseTest {
    protected int file() {
        return 2000;
    }

    public void makeSureLargeDir(String testDir, FileSystem fs) throws Exception {
        if (fs.exists(new Path(testDir + "/" + (file() - 1)))) {
            return;
        }
        ExecutorService service = Executors.newFixedThreadPool(20);
        for (int i = 0; i < file(); i++) {
            Path p = new Path(testDir + "/" + i);
            service.submit(() -> {
                try {
                    fs.create(p).close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        awaitAllExecutors(service);
    }

    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        makeSureLargeDir(testDir, fs);
        service.submit(() -> {
            try {
                fs.delete(new Path(testDir));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        long ms = System.currentTimeMillis();
        awaitAllExecutors(service);
        long useMs = System.currentTimeMillis() - ms;
        // 确保下一次加速
        makeSureLargeDir(testDir, fs);
        return useMs;
    }
}
