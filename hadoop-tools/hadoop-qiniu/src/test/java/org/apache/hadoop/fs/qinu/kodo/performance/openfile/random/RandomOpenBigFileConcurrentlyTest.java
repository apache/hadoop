package org.apache.hadoop.fs.qinu.kodo.performance.openfile.random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomOpenBigFileConcurrentlyTest extends ARandomOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(3);
    }

    @Override
    protected int blockSize() {
        return 4 * 1024 * 1024;
    }

    @Override
    protected int blocks() {
        return 10;
    }

    @Override
    protected int readers() {
        return 3;
    }

    @Override
    protected int randomReadCount() {
        return 100;
    }
}
