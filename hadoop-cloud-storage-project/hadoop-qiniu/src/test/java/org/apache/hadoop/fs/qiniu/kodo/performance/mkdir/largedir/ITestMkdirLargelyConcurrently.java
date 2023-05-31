package org.apache.hadoop.fs.qiniu.kodo.performance.mkdir.largedir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestMkdirLargelyConcurrently extends AMkdirLargelyTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 8;
    }
}
