package org.apache.hadoop.fs.qiniu.kodo.performance.mkdir.deepdir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestMkdirDeeplySerially extends AMkdirDeeplyTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected int consumers() {
        return 1;
    }
}
