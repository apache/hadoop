package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;

public class AuthConfig extends AConfigBase {
    public final String ACCESS_KEY = namespace + ".accessKey";
    public final String SECRET_KEY = namespace + ".secretKey";

    public final String accessKey;
    public final String secretKey;

    public AuthConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.accessKey = accessKey();
        this.secretKey = secretKey();
    }


    public String accessKey() {
        return conf.get(ACCESS_KEY);
    }


    public String secretKey() {
        return conf.get(SECRET_KEY);
    }

}
