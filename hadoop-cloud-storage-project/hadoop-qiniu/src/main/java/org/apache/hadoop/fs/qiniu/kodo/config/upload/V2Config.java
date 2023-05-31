package org.apache.hadoop.fs.qiniu.kodo.config.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class V2Config extends AConfigBase {
    public final boolean enable;
    public final int blockSize;

    public V2Config(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.blockSize = blockSize();
    }

    public boolean enable() {
        return conf.getBoolean(namespace + ".enable", true);
    }

    public int blockSize() {
        return conf.getInt(namespace + ".blockSize", 32 * 1024 * 1024);
    }
}
