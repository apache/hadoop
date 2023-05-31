package org.apache.hadoop.fs.qiniu.kodo.performance.createfile.smallfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class ACreateSmallFileTest extends QiniuKodoPerformanceBaseTest {
    protected int files() {
        return 100;
    }

    abstract protected int consumers();

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("files", files());
        data.put("consumers", consumers());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        long ms = System.currentTimeMillis();

        for (int i = 0; i < files(); i++) {
            final Path p = new Path(testDir + "/" + i);
            service.submit(() -> {
                try {
                    fs.create(p).close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        service.shutdown();
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
