package org.apache.hadoop.fs.qiniu.kodo.performance.createfile.bigfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestCreateBigFileConcurrently extends ACreateBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(consumers());
    }

    @Override
    protected int consumers() {
        return 8;
    }
}
