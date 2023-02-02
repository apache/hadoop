package org.apache.hadoop.fs.qinu.kodo.performance.createfile.bigfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateBigFileConcurrentlyTest extends ACreateBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(8);
    }
}
