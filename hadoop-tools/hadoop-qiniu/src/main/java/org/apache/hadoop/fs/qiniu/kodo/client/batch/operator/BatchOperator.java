package org.apache.hadoop.fs.qiniu.kodo.client.batch.operator;

import com.qiniu.storage.BucketManager.BatchOperations;

public interface BatchOperator {
    void addTo(BatchOperations batchOperations);
}

