package org.apache.hadoop.fs.qiniu.kodo.config.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class ListConfig extends AConfigBase {
    public final boolean useListV2;
    public final int singleRequestLimit;
    public final int bufferSize;
    public final int offerTimeout;

    public ListConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.useListV2 = useListV2();
        this.singleRequestLimit = singleRequestLimit();
        this.bufferSize = bufferSize();
        this.offerTimeout = offerTimeout();
    }

    private boolean useListV2() {
        return conf.getBoolean(namespace + ".useListV2", false);
    }

    private int singleRequestLimit() {
        return conf.getInt(namespace + ".singleRequestLimit", 1000);
    }

    private int bufferSize() {
        return conf.getInt(namespace + ".bufferSize", 100);
    }

    private int offerTimeout() {
        return conf.getInt(namespace + ".offerTimeout", 10);
    }
}
