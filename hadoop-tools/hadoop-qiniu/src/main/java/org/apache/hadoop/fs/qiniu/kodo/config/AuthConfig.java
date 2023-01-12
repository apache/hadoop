package org.apache.hadoop.fs.qiniu.kodo.config;

import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.util.Map;

public class AuthConfig extends AConfigBase{
    public final String ACCESS_KEY = namespace + ".accessKey";
    public final String SECRET_KEY = namespace + ".secret";

    public final String accessKey;
    public final String secretKey;

    public AuthConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        Map<String, String> env = System.getenv();
        conf.setIfUnset(ACCESS_KEY, env.get("QSHELL_AK"));
        conf.setIfUnset(SECRET_KEY, env.get("QSHELL_SK"));
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
