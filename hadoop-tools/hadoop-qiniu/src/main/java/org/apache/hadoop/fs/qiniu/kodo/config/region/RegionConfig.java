package org.apache.hadoop.fs.qiniu.kodo.config.region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.AConfigBase;

public class RegionConfig extends AConfigBase {
    public final String id;

    public final CustomRegionConfig custom;


    public RegionConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.id = id();
        this.custom = new CustomRegionConfig(conf, namespace + ".custom");
    }

    private String id() {
        return conf.get(namespace + ".id");
    }
}
