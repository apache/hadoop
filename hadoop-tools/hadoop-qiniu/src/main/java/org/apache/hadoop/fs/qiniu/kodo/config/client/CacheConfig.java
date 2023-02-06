package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class CacheConfig extends AConfigBase {
    public final boolean enable;

    public CacheConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
    }

    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", true);
    }
}
