package org.apache.hadoop.fs.qiniu.kodo.config;


import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.download.DownloadConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class QiniuKodoFsConfig extends AConfigBase{
    public final String regionId;
    public final boolean useHttps;

    public final AuthConfig auth;
    public final DownloadConfig download;

    public QiniuKodoFsConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.regionId = regionId();
        this.useHttps = useHttps();
        this.auth = auth();
        this.download = download();
    }

    public QiniuKodoFsConfig(Configuration conf) {
        this(conf, "fs.qiniu");
    }


    /**
     * 获取bucket的region配置信息，若为空则自动获取region
     */
    private String regionId() {
        return conf.get(namespace + ".regionId");
    }

    private boolean useHttps() {
        return conf.getBoolean(namespace + ".useHttps", true);
    }

    private AuthConfig auth() {
        return new AuthConfig(conf, namespace + ".auth");
    }

    private DownloadConfig download() {
        return new DownloadConfig(conf, namespace + ".download");
    }
}
