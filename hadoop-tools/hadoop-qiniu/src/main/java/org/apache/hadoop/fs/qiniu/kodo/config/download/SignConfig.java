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


    /**
     * 下载文件是否使用签名
     */
    private boolean enable() {
        return conf.getBoolean(namespace + ".enable", true);
    }

    /**
     * 下载签名过期时间
     */
    private int expires() {
        return conf.getInt(namespace + ".expires", 7 * 24 * 3600);
    }

    @Override
    public String toString() {
        return "SignConfig{" +
                "enable=" + enable +
                ", expires=" + expires +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
