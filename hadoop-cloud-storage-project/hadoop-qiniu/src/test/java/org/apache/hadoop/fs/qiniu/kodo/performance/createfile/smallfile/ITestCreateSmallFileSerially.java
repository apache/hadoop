package org.apache.hadoop.fs.qiniu.kodo.performance.createfile.smallfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestCreateSmallFileSerially extends ACreateSmallFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Override
    protected int consumers() {
        return 1;
    }
}
