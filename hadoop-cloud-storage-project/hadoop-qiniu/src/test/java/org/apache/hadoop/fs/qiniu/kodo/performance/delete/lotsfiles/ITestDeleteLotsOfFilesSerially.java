package org.apache.hadoop.fs.qiniu.kodo.performance.delete.lotsfiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestDeleteLotsOfFilesSerially extends ADeleteLotsOfFilesTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }
}
