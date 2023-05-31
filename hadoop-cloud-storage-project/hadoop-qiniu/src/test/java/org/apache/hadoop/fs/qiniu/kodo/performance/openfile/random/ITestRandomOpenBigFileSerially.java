package org.apache.hadoop.fs.qiniu.kodo.performance.openfile.random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestRandomOpenBigFileSerially extends ARandomOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }


}
