package org.apache.hadoop.fs.qiniu.kodo.client.batch;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.BatchOperations;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.BatchOperator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BatchOperationConsumer implements Runnable {
    private final BlockingQueue<BatchOperator> queue;
    private final BucketManager bucketManager;
    private final int singleBatchRequestLimit;
    private final int pollTimeout;

    private BatchOperations batchOperations = null;
    private int batchOperationsSize = 0;
    private volatile boolean isRunning = true;

    public BatchOperationConsumer(
            BlockingQueue<BatchOperator> queue,
            BucketManager bucketManager,
            int singleBatchRequestLimit,
            int pollTimeout
    ) {
        this.queue = queue;
        this.bucketManager = bucketManager;
        this.singleBatchRequestLimit = singleBatchRequestLimit;
        this.pollTimeout = pollTimeout;
    }

    private void submitBatchOperations() throws QiniuException {
        if (batchOperations == null) {
            return;
        }
        bucketManager.batch(batchOperations);
        batchOperations = null;
        batchOperationsSize = 0;
    }

    private void loop() {
        try {
            BatchOperator operator = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);

            // poll失败了，等待下一次循环轮询
            if (operator == null) {
                return;
            }
            if (batchOperations == null) {
                batchOperations = new BatchOperations();
            }

            operator.addTo(batchOperations);
            batchOperationsSize++;

            // 批处理数目不够，直接等待下一次poll
            if (batchOperationsSize < singleBatchRequestLimit) {
                return;
            }
            // 批处理到到达一定数目时提交
            submitBatchOperations();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        // is Running == true or
        // queue非空
        while (isRunning || !queue.isEmpty()) {
            loop();
        }
        // 提交剩余的批处理
        if (batchOperationsSize > 0) {
            try {
                submitBatchOperations();
            } catch (QiniuException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void stop() {
        isRunning = false;
    }
}
