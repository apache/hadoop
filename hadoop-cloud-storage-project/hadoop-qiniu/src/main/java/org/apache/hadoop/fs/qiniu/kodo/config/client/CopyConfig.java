package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListAndBatchBaseConfig;

public class CopyConfig extends ListAndBatchBaseConfig {
    public CopyConfig(Configuration conf, String namespace) {
        super(conf, namespace);
    }
}
