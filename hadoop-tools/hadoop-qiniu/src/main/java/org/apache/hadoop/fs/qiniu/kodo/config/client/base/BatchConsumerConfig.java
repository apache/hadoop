package org.apache.hadoop.fs.qiniu.kodo.config.client.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class BatchConsumerConfig extends AConfigBase {

    public final int bufferSize;
    public final int count;
    public final int singleBatchRequestLimit;
    public final int pollTimeout;

    public BatchConsumerConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.bufferSize = bufferSize();
        this.count = count();
        this.singleBatchRequestLimit = singleBatchRequestLimit();
        this.pollTimeout = pollTimeout();
    }

    protected int bufferSize() {
        return conf.getInt(namespace + ".bufferSize", 1000);
    }

    protected int count() {
        return conf.getInt(namespace + ".count", 4);
    }

    protected int singleBatchRequestLimit() {
        return conf.getInt(namespace + ".singleBatchRequestLimit", 200);
    }

    protected int pollTimeout() {
        return conf.getInt(namespace + ".pollTimeout", 10);
    }

}
