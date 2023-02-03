package org.apache.hadoop.fs.qinu.kodo.performance.mkdir.largedir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MkdirLargelyConcurrentlyTest extends AMkdirLargelyTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 8;
    }
}
