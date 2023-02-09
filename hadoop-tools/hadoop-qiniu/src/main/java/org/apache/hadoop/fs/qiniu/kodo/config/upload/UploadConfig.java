package org.apache.hadoop.fs.qiniu.kodo.config.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class UploadConfig extends AConfigBase {
    public final SignConfig sign;
    public final int maxConcurrentTasks;
    public final boolean useHttps;
    public final boolean accUpHostFirst;
    public final boolean useDefaultUpHostIfNone;
    public final V2Config v2;
    public final int bufferSize;

    public UploadConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.sign = sign();
        this.maxConcurrentTasks = maxConcurrentTasks();
        this.useHttps = useHttps();
        this.accUpHostFirst = accUpHostFirst();
        this.useDefaultUpHostIfNone = useDefaultUpHostIfNone();
        this.v2 = v2();
        this.bufferSize = bufferSize();
    }

    private int bufferSize() {
        return conf.getInt(namespace + ".bufferSize", 4 * 1024 * 1024);
    }

    private V2Config v2() {
        return new V2Config(conf, namespace + ".v2");
    }

    private boolean useDefaultUpHostIfNone() {
        return conf.getBoolean(namespace + ".useDefaultUpHostIfNone", true);
    }

    private SignConfig sign() {
        return new SignConfig(conf, namespace + ".sign");
    }

    private int maxConcurrentTasks() {
        return conf.getInt(namespace + ".concurrentTasks", 4);
    }

    private boolean useHttps() {
        return conf.getBoolean(namespace + ".useHttps", true);
    }

    private boolean accUpHostFirst() {
        return conf.getBoolean(namespace + ".accUpHostFirst", true);
    }
}
