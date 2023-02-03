package org.apache.hadoop.fs.qinu.kodo.performance.list;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AListBigDirectoryTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(AListBigDirectoryTest.class);

    private void makeSureExistsBigDir(String testDir, FileSystem fs) throws Exception {
        Path tryDetectFile = new Path(testDir + "/" + 9999);
        if (fs.exists(tryDetectFile)) {
            LOG.info("Exist file: {}", tryDetectFile);
            return;
        }
        ExecutorService service = Executors.newFixedThreadPool(10);
        // 创建10000个文件
        for (int i = 0; i < 10000; i++) {
            Path targetFile = new Path(testDir + "/" + i);

            LOG.info("Submit file: {}", targetFile);
            service.submit(() -> {
                try {
                    fs.create(targetFile).close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        service.shutdown();
        awaitAllExecutors(service);
    }


    // 列举次数
    protected int count() {
        return 10;
    }

    // 列举者并发列举的读者数
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
        makeSureExistsBigDir(testDir, fs);
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
