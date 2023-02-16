package org.apache.hadoop.fs.qiniu.kodo.client.batch.operator;

import com.qiniu.storage.BucketManager;

public class CopyOperator implements BatchOperator {
    public final String fromBucket;
    public final String fromFileKey;
    public final String toBucket;
    public final String toFileKey;

    public CopyOperator(
            String fromBucket,
            String fromFileKey,
            String toBucket,
            String toFileKey) {
        this.fromBucket = fromBucket;
        this.fromFileKey = fromFileKey;
        this.toBucket = toBucket;
        this.toFileKey = toFileKey;
    }

    @Override
    public void addTo(BucketManager.BatchOperations batchOperations) {
        batchOperations.addCopyOp(fromBucket, fromFileKey, toBucket, toFileKey);
    }
}
