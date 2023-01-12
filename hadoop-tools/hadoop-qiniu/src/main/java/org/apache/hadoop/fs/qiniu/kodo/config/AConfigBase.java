package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;

public abstract class AConfigBase {
    protected final Configuration conf;
    protected final String namespace;
    public AConfigBase(Configuration conf, String namespace) {
        this.conf = conf;
        this.namespace = namespace;
    }
}
