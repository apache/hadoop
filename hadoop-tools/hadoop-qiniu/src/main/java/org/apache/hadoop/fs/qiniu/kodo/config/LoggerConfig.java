package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;

public class LoggerConfig extends AConfigBase {
    public final String log4jConfigFile;
    public final String logLevel;

    public LoggerConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.log4jConfigFile = log4jConfigFile();
        this.logLevel = logLevel();
    }

    public String log4jConfigFile() {
        return conf.get(namespace + ".log4jConfigFile");
    }

    public String logLevel() {
        return conf.get(namespace + ".logLevel", "INFO");
    }
}
