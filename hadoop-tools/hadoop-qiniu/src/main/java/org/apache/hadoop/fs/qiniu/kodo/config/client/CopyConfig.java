package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class CopyConfig extends AConfigBase {
    public static class ProducerConfig extends AConfigBase {
        public final int queueBufferSize;
        public final int singleRequestLimit;
        public final boolean useV2;
        public final int offerTimeout;

        public ProducerConfig(Configuration conf, String namespace) {
            super(conf, namespace);
            this.queueBufferSize = queueBufferSize();
            this.singleRequestLimit = singleRequestLimit();
            this.useV2 = useV2();
            this.offerTimeout = offerTimeout();
        }

        private int queueBufferSize() {
            return conf.getInt(namespace + ".queueBufferSize", 1000);
        }

        private int singleRequestLimit() {
            return conf.getInt(namespace + ".singleRequestLimit", 800);
        }

        private boolean useV2() {
            return conf.getBoolean(namespace + ".useV2", false);
        }

        private int offerTimeout() {
            return conf.getInt(namespace + ".offerTimeout", 10);
        }
    }

    public static class ConsumerConfig extends AConfigBase {
        public final int queueBufferSize;
        public final int count;
        public final int singleBatchRequestLimit;
        public final int pollTimeout;

        public ConsumerConfig(Configuration conf, String namespace) {
            super(conf, namespace);
            this.queueBufferSize = queueBufferSize();
            this.count = count();
            this.singleBatchRequestLimit = singleBatchRequestLimit();
            this.pollTimeout = pollTimeout();
        }

        private int queueBufferSize() {
            return conf.getInt(namespace + ".queueBufferSize", 1000);
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

    public final ProducerConfig producer;
    public final ConsumerConfig consumer;

    public CopyConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.producer = producer();
        this.consumer = consumer();
    }

    private ProducerConfig producer() {
        return new ProducerConfig(conf, namespace + ".producer");
    }

    private ConsumerConfig consumer() {
        return new ConsumerConfig(conf, namespace + ".consumer");
    }

}
