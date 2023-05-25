package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.client.base.ListAndBatchBaseConfig;

public class RenameConfig extends ListAndBatchBaseConfig {
    public RenameConfig(Configuration conf, String namespace) {
        super(conf, namespace);
    }
}
