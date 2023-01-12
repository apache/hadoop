package org.apache.hadoop.fs.qiniu.kodo.config.download.cache;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class MemoryCacheConfig extends AConfigBase {
    public final boolean enable;
    public final int blocks;
    public MemoryCacheConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.blocks = blocks();
    }

    /**
     * 是否启用内存缓存
     */
    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", true);
    }

    /**
     * 读取文件时内存LRU缓冲区的最大块数量
     */
    private int blocks() {
        return conf.getInt(namespace + ".blocks", 25);
    }

}
