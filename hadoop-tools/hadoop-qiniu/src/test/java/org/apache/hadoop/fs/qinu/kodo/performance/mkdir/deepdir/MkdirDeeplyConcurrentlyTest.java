package org.apache.hadoop.fs.qinu.kodo.performance.mkdir.deepdir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MkdirDeeplyConcurrentlyTest extends AMkdirDeeplyTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 8;
    }
}
