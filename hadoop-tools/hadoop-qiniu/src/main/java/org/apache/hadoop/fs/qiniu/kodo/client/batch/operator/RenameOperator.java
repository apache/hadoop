package org.apache.hadoop.fs.qiniu.kodo.client.batch.operator;

import com.qiniu.storage.BucketManager;

public class RenameOperator implements BatchOperator {
    public final String fromBucket;
    public final String fromFileKey;
    public final String toFileKey;

    public RenameOperator(String fromBucket, String fromFileKey, String toFileKey) {
        this.fromBucket = fromBucket;
        this.fromFileKey = fromFileKey;
        this.toFileKey = toFileKey;
    }

    @Override
    public void addTo(BucketManager.BatchOperations batchOperations) {
        batchOperations.addRenameOp(fromBucket, fromFileKey, toFileKey);
    }
}
