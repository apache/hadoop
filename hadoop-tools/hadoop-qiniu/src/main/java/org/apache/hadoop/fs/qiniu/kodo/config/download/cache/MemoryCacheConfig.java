package org.apache.hadoop.fs.qiniu.kodo.config.download.cache;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class MemoryCacheConfig extends AConfigBase {
    public final int blocks;

    public MemoryCacheConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.blocks = blocks();
    }

    /**
     * 读取文件时内存LRU缓冲区的最大块数量
     */
    private int blocks() {
        return conf.getInt(namespace + ".blocks", 25);
    }

    @Override
    public String toString() {
        return "MemoryCacheConfig{" +
                "blocks=" + blocks +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}