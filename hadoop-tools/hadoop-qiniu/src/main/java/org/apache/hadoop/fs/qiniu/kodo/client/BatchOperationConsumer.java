package org.apache.hadoop.fs.qiniu.kodo.client;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.BatchOperations;
import org.apache.hadoop.fs.qiniu.kodo.client.operator.BatchOperator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BatchOperationConsumer implements Runnable {
    private final BlockingQueue<BatchOperator> queue;
    private final BucketManager bucketManager;
    private final int singleBatchRequestLimit;

    private BatchOperations batchOperations = null;
    private int batchOperationsSize = 0;
    private boolean isRunning = true;

    public BatchOperationConsumer(
            BlockingQueue<BatchOperator> queue,
            BucketManager bucketManager,
            int singleBatchRequestLimit
    ) {
        this.queue = queue;
        this.bucketManager = bucketManager;
        this.singleBatchRequestLimit = singleBatchRequestLimit;
    }

    private void submitBatchOperations() throws QiniuException {
        if (batchOperations == null) return;
        bucketManager.batch(batchOperations);
        batchOperations = null;
        batchOperationsSize = 0;
    }

    private void loop() {
        try {
            BatchOperator operator = queue.poll(1, TimeUnit.SECONDS);

            // poll失败了，等待下一次循环轮询
            if (operator == null) return;
            if (batchOperations == null) batchOperations = new BatchOperations();

            operator.addTo(batchOperations);
            batchOperationsSize++;

            // 批处理到到达一定数目时提交
            if (batchOperationsSize >= singleBatchRequestLimit) {
                submitBatchOperations();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (isRunning) {
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
