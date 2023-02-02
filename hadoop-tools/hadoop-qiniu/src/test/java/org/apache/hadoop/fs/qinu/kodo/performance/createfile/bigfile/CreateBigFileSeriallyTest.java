package org.apache.hadoop.fs.qinu.kodo.performance.createfile.bigfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateBigFileSeriallyTest extends ACreateBigFileTest {
    @Override
    protected ExecutorService getExecutorService() {
        return Executors.newSingleThreadExecutor();
    }
}
