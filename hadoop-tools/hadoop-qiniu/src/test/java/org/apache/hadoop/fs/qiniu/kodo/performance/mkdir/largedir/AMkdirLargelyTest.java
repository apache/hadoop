package org.apache.hadoop.fs.qiniu.kodo.performance.mkdir.largedir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class AMkdirLargelyTest extends QiniuKodoPerformanceBaseTest {
    protected int dirs() {
        return 100;
    }

    abstract protected int consumers();

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("dirs", dirs());
        data.put("consumers", consumers());

        return data;
    }

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
