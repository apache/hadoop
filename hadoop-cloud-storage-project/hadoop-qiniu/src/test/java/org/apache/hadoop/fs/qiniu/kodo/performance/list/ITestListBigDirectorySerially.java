package org.apache.hadoop.fs.qiniu.kodo.performance.list;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestListBigDirectorySerially extends AListBigDirectoryTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected int readers() {
        return 1;
    }
}
