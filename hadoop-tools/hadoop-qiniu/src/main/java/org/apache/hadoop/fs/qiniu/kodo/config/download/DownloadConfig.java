package org.apache.hadoop.fs.qiniu.kodo.config.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.CacheConfig;

public class DownloadConfig extends AConfigBase {
    public final CacheConfig cache;
    public final int blockSize;
    public final String domain;
    public final SignConfig sign;
    public DownloadConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.cache = cache();
        this.blockSize = blockSize();
        this.domain = domain();
        this.sign = new SignConfig(conf, namespace + ".sign");
    }

    private CacheConfig cache() {
        return new CacheConfig(conf, namespace + ".cache");
    }

    /**
     * 读取文件时下载块大小
     * 默认为4M
     */
    private int blockSize() {
        return conf.getInt(namespace + ".blockSize", 4 * 1024 * 1024);
    }

    /**
     * 若返回空，则默认走源站
     */
    private String domain() {
        return conf.get(namespace + ".domain");
    }

    @Override
    public String toString() {
        return "DownloadConfig{" +
                "cache=" + cache +
                ", blockSize=" + blockSize +
                ", domain='" + domain + '\'' +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
