package org.apache.hadoop.fs.qiniu.kodo.config.customregion;

import com.qiniu.storage.Region;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;
import org.apache.hadoop.fs.qiniu.kodo.config.MissingConfigFieldException;

public class CustomRegionConfig extends AConfigBase {
    public final String id;

    public final CustomRegionItemsConfig custom;


    public CustomRegionConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.id = id();
        this.custom = new CustomRegionItemsConfig(conf, namespace + ".custom");
    }

    private String id() {
        return conf.get(namespace + ".id");
    }

    public Region getCustomRegion() throws MissingConfigFieldException {
        return this.custom.buildCustomSdkRegion(this.id);
    }
}
