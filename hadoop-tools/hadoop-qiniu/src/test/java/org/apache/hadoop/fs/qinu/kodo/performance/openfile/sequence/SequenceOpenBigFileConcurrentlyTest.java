package org.apache.hadoop.fs.qinu.kodo.performance.openfile.sequence;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SequenceOpenBigFileConcurrentlyTest extends ASequenceOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(2);
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
        return 2;
    }
}
