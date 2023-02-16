package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListAndBatchBaseConfig;

public class DeleteConfig extends ListAndBatchBaseConfig {
    public DeleteConfig(Configuration conf, String namespace) {
        super(conf, namespace);
    }
}
