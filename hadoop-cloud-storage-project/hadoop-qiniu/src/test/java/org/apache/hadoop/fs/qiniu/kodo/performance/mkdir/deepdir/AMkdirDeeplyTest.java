package org.apache.hadoop.fs.qiniu.kodo.performance.mkdir.deepdir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class AMkdirDeeplyTest extends QiniuKodoPerformanceBaseTest {
    protected int dirs() {
        return 10;
    }

    protected int deep() {
        return 10;
    }

    abstract protected int consumers();

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("dirs", dirs());
        data.put("deep", deep());
        data.put("consumers", consumers());

        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        long ms = System.currentTimeMillis();
        for (int i = 0; i < dirs(); i++) {
            StringBuilder d1 = new StringBuilder(testDir + "/" + i);
            for (int j = 0; j < deep(); j++) {
                d1.append("/").append(j);
            }
            final Path p = new Path(d1.toString());
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
