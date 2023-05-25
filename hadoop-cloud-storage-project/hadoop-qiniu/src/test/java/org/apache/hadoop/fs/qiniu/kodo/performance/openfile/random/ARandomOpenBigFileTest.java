package org.apache.hadoop.fs.qiniu.kodo.performance.openfile.random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class ARandomOpenBigFileTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ARandomOpenBigFileTest.class);


    protected int blockSize() {
        return 4 * 1024 * 1024;
    }

    protected int blocks() {
        return 10;
    }

    protected int readers() {
        return 3;
    }

    protected int randomReadCount() {
        return 100;
    }

    @Override
    protected Map<String, Object> testInputData() {
        Map<String, Object> data = new HashMap<>();
        data.put("blockSize", blockSize());
        data.put("blocks", blocks());
        data.put("readers", readers());
        data.put("randomReadCount", randomReadCount());
        return data;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        String path = testDir + "/bigFile";
        getPrepareHelper().prepareBigFile(path, blockSize() * blocks());

        for (int i = 0; i < readers(); i++) {
            service.submit(() -> {
                try {
                    FSDataInputStream fis = fs.open(new Path(path));
                    int len = fis.available();
                    Assert.assertEquals(blockSize() * blocks(), len);

                    for (int j = 0; j < randomReadCount(); j++) {
                        int pos = (int) (Math.random() * len);
                        fis.seek(pos);
                        int b = fis.read();
                    }

                    fis.close();
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
