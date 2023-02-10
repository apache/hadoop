package org.apache.hadoop.fs.qinu.kodo.performance.delete.lotsfiles;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DeleteLotsOfFilesSeriallyTest extends ADeleteLotsOfFilesTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }
}
