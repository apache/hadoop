package org.apache.hadoop.fs.qiniu.kodo;


import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.io.IOException;
import java.util.Map;

public class QiniuKodoFsConfig {
    public static final String QINIU_PARAMETER_ACCESS_KEY = "fs.qiniu.access.key";

    public static final String QINIU_PARAMETER_SECRET_KEY = "fs.qiniu.secret.key";
    private final Configuration conf;

    public QiniuKodoFsConfig(Configuration conf) {
        this.conf = conf;
        Map<String, String> env = System.getenv();
        conf.setIfUnset(QINIU_PARAMETER_ACCESS_KEY, env.get("QSHELL_AK"));
        conf.setIfUnset(QINIU_PARAMETER_SECRET_KEY, env.get("QSHELL_SK"));
    }

    public String getAuthAccessKey() throws AuthorizationException {
        String accessKey = conf.get(QINIU_PARAMETER_ACCESS_KEY);
        if (!StringUtils.isNullOrEmpty(accessKey)) return accessKey;

        throw new AuthorizationException(String.format(
                "Qiniu access key can't empty, you should set it with %s in core-site.xml",
                QINIU_PARAMETER_ACCESS_KEY
        ));
    }

    public String getAuthSecretKey() throws AuthorizationException {
        String secretKey = conf.get(QINIU_PARAMETER_SECRET_KEY);
        if (!StringUtils.isNullOrEmpty(secretKey)) return secretKey;
        throw new AuthorizationException(String.format(
                "Qiniu secret key can't empty, you should set it with %s in core-site.xml",
                QINIU_PARAMETER_SECRET_KEY
        ));
    }

    public Auth createAuth() throws AuthorizationException {
        return Auth.create(getAuthAccessKey(), getAuthSecretKey());
    }

    public String getRegionId() {
        return conf.get("fs.qiniu.region.id");
    }

    public int getBlockSize() {
        return conf.getInt("fs.qiniu.block.size", 4 * 1024 * 1024);
    }

    public long getMultipartDownloadSize() {
        return conf.getLong("fs.qiniu.multipart.download.size", 512 * 1024);
    }

    public String getBufferDir() {
        return conf.get("fs.qiniu.buffer.dir");
    }
}
