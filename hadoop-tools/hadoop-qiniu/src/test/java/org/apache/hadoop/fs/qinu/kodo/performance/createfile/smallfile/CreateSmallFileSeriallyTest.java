package org.apache.hadoop.fs.qinu.kodo.performance.createfile.smallfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateSmallFileSeriallyTest extends ACreateSmallFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected int files() {
        return 100;
    }
}
