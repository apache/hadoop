package org.apache.hadoop.fs.qinu.kodo.performance.createfile.smallfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateSmallFileConcurrentlyTest extends ACreateSmallFileTest {
    @Override
    protected ExecutorService getExecutorService() {
        return Executors.newFixedThreadPool(8);
    }
}
