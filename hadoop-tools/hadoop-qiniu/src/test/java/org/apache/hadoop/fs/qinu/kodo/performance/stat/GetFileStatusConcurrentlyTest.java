package org.apache.hadoop.fs.qinu.kodo.performance.stat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GetFileStatusConcurrentlyTest extends AGetFileStatusTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 8;
    }
}
