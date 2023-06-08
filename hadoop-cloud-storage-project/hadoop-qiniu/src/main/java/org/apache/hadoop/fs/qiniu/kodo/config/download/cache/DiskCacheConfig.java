package org.apache.hadoop.fs.qiniu.kodo.config.download.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DiskCacheConfig extends AConfigBase {
    public final boolean enable;
    public final int blocks;
    public final Path dir;
    public final int expires;

    public DiskCacheConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.blocks = blocks();
        this.dir = dir();
        this.expires = expires();
    }

    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", false);
    }

    private int blocks() {
        return conf.getInt(namespace + ".blocks", 120);
    }

    private Path dir() {
        String dir = conf.get(namespace + ".dir");
        if (dir != null) {
            return Paths.get(dir);
        }

        String hadoopTmpDir = conf.get("hadoop.tmp.dir");
        return Paths.get(hadoopTmpDir, "qiniu", "download");
    }

    private int expires() {
        return conf.getInt(namespace + ".expires", 24 * 3600);
    }
}
