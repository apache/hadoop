package org.apache.hadoop.fs.qinu.kodo.performance.createfile.bigfile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateBigFileConcurrentlyTest extends ACreateBigFileTest {
    @Override
    protected ExecutorService buildExecutorService() {
        return Executors.newFixedThreadPool(8);
    }

    @Override
    protected int files() {
        // 创建5个文件
        return 5;
    }

    @Override
    protected int blockSize() {
        // 每块4MB
        return 4 * 1024 * 1024;
    }

    @Override
    protected int blocks() {
        // 每个文件2块，预计耗费上传流量 40MB
        return 2;
    }

    @Override
    protected long timeoutN() {
        return 2;
    }
}
