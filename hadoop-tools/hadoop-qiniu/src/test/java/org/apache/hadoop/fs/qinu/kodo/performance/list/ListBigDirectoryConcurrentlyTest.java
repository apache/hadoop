package org.apache.hadoop.fs.qinu.kodo.performance.list;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ListBigDirectoryConcurrentlyTest extends AListBigDirectoryTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(readers());
    }


    @Override
    protected int count() {
        return 10;
    }

    @Override
    protected int readers() {
        return 4;
    }
}
