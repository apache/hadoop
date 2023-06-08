package org.apache.hadoop.fs.qiniu.kodo.config;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.client.ClientConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.customregion.CustomRegionConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.download.DownloadConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.upload.UploadConfig;

public class QiniuKodoFsConfig extends AConfigBase {
    public final AuthConfig auth;

    public final DownloadConfig download;
    public final UploadConfig upload;
    public final CustomRegionConfig customRegion;
    public final ClientConfig client;
    public final ProxyConfig proxy;
    public final boolean useHttps;
    public final LoggerConfig logger;

    public QiniuKodoFsConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.customRegion = region();
        this.auth = auth();
        this.download = download();
        this.upload = upload();
        this.client = client();
        this.proxy = proxy();
        this.useHttps = useHttps();
        this.logger = logger();
    }

    public QiniuKodoFsConfig(Configuration conf) {
        this(conf, "fs.qiniu");
    }


    private CustomRegionConfig region() {
        return new CustomRegionConfig(conf, namespace + ".customRegion");
    }

    private AuthConfig auth() {
        return new AuthConfig(conf, namespace + ".auth");
    }

    private DownloadConfig download() {
        return new DownloadConfig(conf, namespace + ".download");
    }

    private UploadConfig upload() {
        return new UploadConfig(conf, namespace + ".upload");
    }

    private ClientConfig client() {
        return new ClientConfig(conf, namespace + ".client");
    }

    private ProxyConfig proxy() {
        return new ProxyConfig(conf, namespace + ".proxy");
    }

    private boolean useHttps() {
        return conf.getBoolean(namespace + ".useHttps", true);
    }

    private LoggerConfig logger() {
        return new LoggerConfig(conf, namespace + ".logger");
    }
}
