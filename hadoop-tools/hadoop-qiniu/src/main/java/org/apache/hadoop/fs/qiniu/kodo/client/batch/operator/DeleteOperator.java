package org.apache.hadoop.fs.qiniu.kodo.client.batch.operator;

import com.qiniu.storage.BucketManager;

public class DeleteOperator implements BatchOperator {
    public final String bucket;
    public final String key;

    public DeleteOperator(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void addTo(BucketManager.BatchOperations batchOperations) {
        batchOperations.addDeleteOp(bucket, key);
    }
}
