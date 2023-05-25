package org.apache.hadoop.fs.qiniu.kodo.performance.rename.largedir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.performance.QiniuKodoPerformanceBaseTest;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestRenameLargeDir extends QiniuKodoPerformanceBaseTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs, ExecutorService service) throws Exception {
        String oldPath = testDir + "/oldDir";
        String newPath = testDir + "/newDir";
        getPrepareHelper().prepareLargeDir(oldPath, 10000);
        service.submit(() -> {
            try {
                fs.rename(new Path(oldPath), new Path(newPath));
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
