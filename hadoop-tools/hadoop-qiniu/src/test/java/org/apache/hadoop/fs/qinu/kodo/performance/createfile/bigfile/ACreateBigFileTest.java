package org.apache.hadoop.fs.qinu.kodo.performance.createfile.bigfile;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class ACreateBigFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ACreateBigFileTest.class);


    abstract protected int consumers();


    protected int files() {
        // 创建5个文件
        return 5;
    }

    protected int blockSize() {
        // 每块4MB
        return 4 * 1024 * 1024;
    }

    protected int blocks() {
        // 每个文件2块，预计耗费上传流量 40MB
        return 2;
    }

    @Override
    protected long timeoutN() {
        return 2;
    }

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("files", files());
        data.put("blockSize", blocks());
        data.put("blocks", blocks());
        data.put("consumers", consumers());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        // 总计20 * 4MB * 2 = 160MB
        byte[] bs = new byte[blockSize()];

        // 建立父目录
        fs.mkdirs(new Path(testDir));

        // 生产
        long ms = System.currentTimeMillis();

        for (int i = 0; i < files(); i++) {
            final Path p = new Path(testDir + "/" + i);
            service.submit(() -> {
                try {
                    FSDataOutputStream fos = fs.create(p);
                    for (int j = 0; j < blocks(); j++) {
                        fos.write(bs);
                    }
                    fos.close();
                    LOG.debug("task create file terminated: {}", p);
                } catch (IOException ex) {
                    LOG.error("io exception: ", ex);
                    throw new RuntimeException(ex);
                }
            });
            LOG.debug("submit task create file: {}", p);
        }
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
