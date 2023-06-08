package org.apache.hadoop.fs.qiniu.kodo.config.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;
import org.apache.hadoop.fs.qiniu.kodo.config.download.cache.CacheConfig;

public class DownloadConfig extends AConfigBase {
    public final String KEY_DOMAIN;

    public final String domain;

    public final CacheConfig cache;
    public final int blockSize;


    public final SignConfig sign;
    public final boolean useNoCacheHeader;
    public final boolean useHttps;
    public final RandomConfig random;

    public DownloadConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.cache = cache();
        this.blockSize = blockSize();

        this.KEY_DOMAIN = namespace + ".domain";
        this.domain = conf.get(namespace + ".domain");

        this.sign = new SignConfig(conf, namespace + ".sign");
        this.useNoCacheHeader = useNoCacheHeader();
        this.useHttps = useHttps();
        this.random = random();
    }

    private RandomConfig random() {
        return new RandomConfig(conf, namespace + ".random");
    }

    private boolean useHttps() {
        return conf.getBoolean(namespace + ".useHttps", true);
    }


    private CacheConfig cache() {
        return new CacheConfig(conf, namespace + ".cache");
    }

    private int blockSize() {
        return conf.getInt(namespace + ".blockSize", 4 * 1024 * 1024);
    }

    private boolean useNoCacheHeader() {
        return conf.getBoolean(namespace + ".useNoCacheHeader", false);
    }

}
