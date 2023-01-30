package org.apache.hadoop.fs.qiniu.kodo.config.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class UploadConfig extends AConfigBase {
    public final SignConfig sign;
    public UploadConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.sign = new SignConfig(conf, namespace + ".sign");
    }
}
