package org.apache.hadoop.fs.qinu.kodo.performance.list;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ListBigDirectorySeriallyTest extends AListBigDirectoryTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }


    @Override
    protected int count() {
        return 10;
    }

    @Override
    protected int readers() {
        return 1;
    }
}
