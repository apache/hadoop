package org.apache.hadoop.fs.qiniu.kodo.config.download.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class CacheConfig extends AConfigBase {
    public final DiskCacheConfig disk;
    public final MemoryCacheConfig memory;

    public CacheConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.disk = disk();
        this.memory = memory();
    }

    private DiskCacheConfig disk() {
        return new DiskCacheConfig(conf, namespace + ".disk");
    }

    private MemoryCacheConfig memory() {
        return new MemoryCacheConfig(conf, namespace + ".memory");
    }

}
