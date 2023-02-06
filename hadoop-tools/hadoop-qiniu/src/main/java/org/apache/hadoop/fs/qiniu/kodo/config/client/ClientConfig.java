package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class ClientConfig extends AConfigBase {
    public final CacheConfig cache;

    public ClientConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.cache = cache();
    }

    private CacheConfig cache() {
        return new CacheConfig(conf, namespace + ".cache");
    }
}
