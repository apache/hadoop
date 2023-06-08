package org.apache.hadoop.fs.qiniu.kodo.config.download;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class SignConfig extends AConfigBase {
    public final boolean enable;
    public final int expires;

    public SignConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.enable = enable();
        this.expires = expires();
    }


    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", true);
    }

    private int expires() {
        return conf.getInt(namespace + ".expires", 3 * 60);
    }

}
