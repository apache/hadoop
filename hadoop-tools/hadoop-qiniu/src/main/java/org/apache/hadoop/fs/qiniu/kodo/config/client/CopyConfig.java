package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class CopyConfig extends AConfigBase {
    public final ListProducerConfig listProducer;
    public final BatchConsumerConfig batchConsumer;

    public CopyConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.listProducer = listProducer();
        this.batchConsumer = batchConsumer();
    }

    private ListProducerConfig listProducer() {
        return new ListProducerConfig(conf, namespace + ".producer");
    }

    private BatchConsumerConfig batchConsumer() {
        return new BatchConsumerConfig(conf, namespace + ".consumer");
    }

}
