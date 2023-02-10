package org.apache.hadoop.fs.qinu.kodo.performance.delete.lotsfiles;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ADeleteLotsOfFilesTest extends QiniuKodoPerformanceBaseTest {
    public int files() {
        return 1000;
    }

    public void makeSureLargeDir(String testDir, FileSystem fs) throws Exception {
        if (fs.exists(new Path(testDir + "/" + (files() - 1)))) {
            return;
        }
        ExecutorService service = Executors.newFixedThreadPool(10);
        for (int i = 0; i < files(); i++) {
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
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        makeSureLargeDir(testDir, fs);

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
