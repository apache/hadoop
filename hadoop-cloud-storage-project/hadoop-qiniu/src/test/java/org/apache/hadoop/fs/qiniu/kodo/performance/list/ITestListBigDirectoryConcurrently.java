package org.apache.hadoop.fs.qiniu.kodo.performance.list;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestListBigDirectoryConcurrently extends AListBigDirectoryTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(readers());
    }

    @Override
    protected int readers() {
        return 4;
    }
}
