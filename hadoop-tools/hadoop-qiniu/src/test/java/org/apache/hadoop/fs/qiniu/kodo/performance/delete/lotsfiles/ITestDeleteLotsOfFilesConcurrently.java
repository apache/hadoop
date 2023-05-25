package org.apache.hadoop.fs.qiniu.kodo.performance.delete.lotsfiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestDeleteLotsOfFilesConcurrently extends ADeleteLotsOfFilesTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(4);
    }
}
