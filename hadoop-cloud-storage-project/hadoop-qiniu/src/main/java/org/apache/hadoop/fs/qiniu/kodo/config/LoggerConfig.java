package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;

public class LoggerConfig extends AConfigBase {
    public final String level;

    public LoggerConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.level = level();
    }


    public String level() {
        return conf.get(namespace + ".level", "INFO");
    }
}
