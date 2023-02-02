package org.apache.hadoop.fs.qinu.kodo.performance.mkdir.deepdir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MkdirDeeplySeriallyTest extends AMkdirDeeplyTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }


    @Override
    protected int dirs() {
        return 10;
    }

    @Override
    protected int deep() {
        return 10;
    }
}
