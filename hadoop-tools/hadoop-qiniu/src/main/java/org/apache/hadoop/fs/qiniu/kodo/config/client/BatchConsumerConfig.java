package org.apache.hadoop.fs.qiniu.kodo.config.client;

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

    private int bufferSize() {
        return conf.getInt(namespace + ".bufferSize", 1000);
    }

    private int count() {
        return conf.getInt(namespace + ".count", 4);
    }

    private int singleBatchRequestLimit() {
        return conf.getInt(namespace + ".singleBatchRequestLimit", 200);
    }

    private int pollTimeout() {
        return conf.getInt(namespace + ".pollTimeout", 10);
    }

}
