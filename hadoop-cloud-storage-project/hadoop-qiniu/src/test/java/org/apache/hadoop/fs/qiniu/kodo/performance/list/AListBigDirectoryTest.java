package org.apache.hadoop.fs.qiniu.kodo.performance.list;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class AListBigDirectoryTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(AListBigDirectoryTest.class);

    protected int count() {
        return 10;
    }

    protected int files() {
        return 10000;
    }

    abstract protected int readers();

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("count", count());
        data.put("readers", readers());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        getPrepareHelper().prepareLargeDir(testDir, files());
        long ms = System.currentTimeMillis();
        for (int i = 0; i < count(); i++) {
            service.submit(() -> {
                try {
                    FileStatus[] ps = fs.listStatus(new Path(testDir));
                    LOG.info("List length: {}", ps.length);
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
