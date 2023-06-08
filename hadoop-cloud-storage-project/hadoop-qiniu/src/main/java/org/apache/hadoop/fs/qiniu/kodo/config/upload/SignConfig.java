package org.apache.hadoop.fs.qiniu.kodo.config.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class SignConfig extends AConfigBase {
    public final int expires;

    public SignConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.expires = expires();
    }

    private int expires() {
        return conf.getInt(namespace + ".expires", 7 * 24 * 3600);
    }
}
