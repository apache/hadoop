package org.apache.hadoop.fs.qiniu.kodo.client.batch;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.BatchOperations;
import org.apache.hadoop.fs.qiniu.kodo.client.batch.operator.BatchOperator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BatchOperationConsumer implements Callable<Exception> {
    private final BlockingQueue<BatchOperator> queue;
    private final BucketManager bucketManager;
    private final int singleBatchRequestLimit;
    private final int pollTimeout;

    private BatchOperations batchOperations = null;
    private int batchOperationsSize = 0;
    private volatile boolean isRunning = true;

    /**
     * The consumer of batch operations.
     *
     * @param queue                   the queue of batch operations
     * @param bucketManager           the bucket manager
     * @param singleBatchRequestLimit the limit of single batch request
     * @param pollTimeout             the timeout of poll
     */
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

    private void loop() throws InterruptedException, QiniuException {
        BatchOperator operator = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);

        // If poll result is null, it means that the timeout has been reached.
        // So we should stop the loop and wait for the next loop.
        if (operator == null) {
            return;
        }
        if (batchOperations == null) {
            batchOperations = new BatchOperations();
        }

        operator.addTo(batchOperations);
        batchOperationsSize++;

        // The number of batch operations is not enough, just wait for the next poll.
        if (batchOperationsSize < singleBatchRequestLimit) {
            return;
        }
        // The number of batch operations is enough, submit the batch operations.
        submitBatchOperations();
    }

    @Override
    public Exception call() {
        try {
            // is Running == true or queue is not empty
            while (isRunning || !queue.isEmpty()) {
                loop();
            }
            // isRunning is false && queue is empty
            if (batchOperationsSize > 0) {
                try {
                    submitBatchOperations();
                } catch (QiniuException e) {
                    return e;
                }
            }
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    public void stop() {
        isRunning = false;
    }
}
