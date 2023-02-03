package org.apache.hadoop.fs.qinu.kodo.performance.openfile.sequence;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SequenceOpenBigFileSeriallyTest extends ASequenceOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newSingleThreadExecutor();
    }


}
