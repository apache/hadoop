package org.apache.hadoop.fs.qiniu.kodo.performance.openfile.sequence;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ITestSequenceOpenBigFileConcurrently extends ASequenceOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(readers());
    }

    @Override
    protected int readers() {
        return 4;
    }
}
