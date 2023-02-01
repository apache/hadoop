package org.apache.hadoop.fs.qinu.kodo.performance.createfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

public class CreateSmallFileConcurrentlyTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateSmallFileTest.class);
    private static final BlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);
    private static final ExecutorService service = Executors.newCachedThreadPool();


    private static long createLargeAmountSmall(String workDir, FileSystem fs) throws Exception {
        final String dir = workDir + "/testCreateLargeAmountSmallFilesConcurrently/";

        for (int i = 0; i < 8; i++) {
            service.submit(() -> {
                while (true) {
                    try {
                        Integer e = queue.poll(2, TimeUnit.SECONDS);
                        if (e == null) {
                            // 如果超过1s收不到数据，那就退出线程
                            break;
                        }

                        fs.create(new Path(dir + "/" + e)).close();

                    } catch (InterruptedException | IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }
        // 建立父目录
        fs.mkdirs(new Path(dir));

        // 生产
        long ms = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            boolean success;
            do {
                success = queue.offer(i, 1, TimeUnit.SECONDS);
            } while (!success);
        }

        return System.currentTimeMillis() - ms;
    }

    @Test
    public void testS3A() throws Exception {
        long time = createLargeAmountSmall(s3aTestDir, s3a);
        LOG.info("time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = createLargeAmountSmall(kodoTestDir, kodo);
        LOG.info("time: " + time);
    }
}
