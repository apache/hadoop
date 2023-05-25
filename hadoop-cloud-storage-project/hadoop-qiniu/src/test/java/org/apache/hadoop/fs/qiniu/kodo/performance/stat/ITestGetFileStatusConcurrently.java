package org.apache.hadoop.fs.qiniu.kodo.performance.stat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestGetFileStatusConcurrently extends AGetFileStatusTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 4;
    }
}
