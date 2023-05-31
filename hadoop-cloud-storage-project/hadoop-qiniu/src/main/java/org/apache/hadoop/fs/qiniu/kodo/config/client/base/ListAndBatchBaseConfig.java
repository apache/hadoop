package org.apache.hadoop.fs.qiniu.kodo.config.client.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class ListAndBatchBaseConfig extends AConfigBase {
    public final ListProducerConfig listProducer;
    public final BatchConsumerConfig batchConsumer;

    public ListAndBatchBaseConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.listProducer = listProducer();
        this.batchConsumer = batchConsumer();
    }

    protected ListProducerConfig listProducer() {
        return new ListProducerConfig(conf, namespace + ".producer");
    }

    protected BatchConsumerConfig batchConsumer() {
        return new BatchConsumerConfig(conf, namespace + ".consumer");
    }

}
