package org.apache.hadoop.fs.qinu.kodo.performance.openfile.random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomOpenBigFileConcurrentlyTest extends ARandomOpenBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(3);
    }
    
}
