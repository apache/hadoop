package org.apache.hadoop.fs.qiniu.kodo.performance.openfile.sequence;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.apache.hadoop.fs.qiniu.kodo.performance.openfile.OpenBigFileCommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class ASequenceOpenBigFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ASequenceOpenBigFileTest.class);


    protected int blockSize() {
        return 4 * 1024 * 1024;
    }

    protected int blocks() {
        return 10;
    }

    abstract protected int readers();

    protected int count() {
        return 10;
    }

    protected int readerBufferSize() {
        return 16 * 1024 * 1024;
    }

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("blockSize", blockSize());
        data.put("blocks", blocks());
        data.put("readers", readers());
        data.put("readerBufferSize", readerBufferSize());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        Path p = OpenBigFileCommonUtil.makeSureExistsBigFile(testDir, fs, blockSize(), blocks());


        for (int i = 0; i < count(); i++) {
            service.submit(() -> {
                try {
                    FSDataInputStream fis = fs.open(p);
                    byte[] buf = new byte[readerBufferSize()];

                    boolean eof;
                    do {
                        eof = fis.read(buf) == -1;
                    } while (!eof);
                    fis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            LOG.debug("submit task create file: {}", p);
        }
        long ms = System.currentTimeMillis();
        awaitAllExecutors(service);
        return System.currentTimeMillis() - ms;
    }
}
