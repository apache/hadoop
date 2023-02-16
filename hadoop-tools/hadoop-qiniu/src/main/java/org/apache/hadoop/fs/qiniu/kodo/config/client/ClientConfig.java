package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class ClientConfig extends AConfigBase {
    public final CacheConfig cache;
    public final ListProducerConfig list;
    public final CopyConfig copy;

    public ClientConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.cache = cache();
        this.list = list();
        this.copy = copy();
    }

    private CacheConfig cache() {
        return new CacheConfig(conf, namespace + ".cache");
    }

    private ListProducerConfig list() {
        return new ListProducerConfig(conf, namespace + ".list");
    }

    private CopyConfig copy() {
        return new CopyConfig(conf, namespace + ".copy");
    }
}
