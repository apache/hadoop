package org.apache.hadoop.fs.qiniu.kodo.performance.rename.bigfile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestRenameBigFile extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ITestRenameBigFile.class);

    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        String oldFile = testDir + "/oldBigFile";
        String newFile = testDir + "/newBigFile";
        getPrepareHelper().prepareBigFile(oldFile, 4 * 1024 * 1024);
        service.submit(() -> {
            try {
                for (int i = 0; i < 2; i++) {
                    LOG.info("rename from {} to {}", oldFile, newFile);
                    fs.rename(new Path(oldFile), new Path(newFile));
                    LOG.info("rename from {} to {}", newFile, oldFile);
                    fs.rename(new Path(newFile), new Path(oldFile));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        long ms = System.currentTimeMillis();
        awaitAllExecutors(service);
        fs.close();
        return System.currentTimeMillis() - ms;
    }
}
