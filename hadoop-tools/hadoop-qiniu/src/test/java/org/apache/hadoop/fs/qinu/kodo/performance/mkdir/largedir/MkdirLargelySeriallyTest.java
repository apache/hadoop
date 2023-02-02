package org.apache.hadoop.fs.qinu.kodo.performance.mkdir.largedir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MkdirLargelySeriallyTest extends AMkdirLargelyTest {
    @Override
    protected ExecutorService getExecutorService() {
        return Executors.newSingleThreadExecutor();
    }
}
