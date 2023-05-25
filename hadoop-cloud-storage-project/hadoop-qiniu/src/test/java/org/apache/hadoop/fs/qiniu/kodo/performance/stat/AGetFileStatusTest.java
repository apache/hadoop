package org.apache.hadoop.fs.qiniu.kodo.performance.stat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class AGetFileStatusTest extends QiniuKodoPerformanceBaseTest {
    protected int count() {
        return 100;
    }

    abstract protected int consumers();

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("count", count());
        data.put("consumers", consumers());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        final Path p = new Path(testDir + "/testFile");
        fs.create(p).close();
        for (int i = 0; i < count(); i++) {
            service.submit(() -> {
                try {
                    fs.getFileStatus(p);
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
