package org.apache.hadoop.fs.qiniu.kodo.config.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class RandomConfig extends AConfigBase {
    public final boolean enable;
    public final int blockSize;
    public final int maxBlocks;

    public RandomConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.blockSize = blockSize();
        this.maxBlocks = maxBlocks();
    }

    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", false);
    }

    private int blockSize() {
        return conf.getInt(namespace + ".blockSize", 64 * 1024);
    }

    private int maxBlocks() {
        return conf.getInt(namespace + ".maxBlocks", 100);
    }
}
