package org.apache.hadoop.fs.qiniu.kodo.config;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.download.DownloadConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.region.RegionConfig;
import org.apache.hadoop.fs.qiniu.kodo.config.upload.UploadConfig;

public class QiniuKodoFsConfig extends AConfigBase {
    public final boolean useHttps;

    public final AuthConfig auth;
    public final DownloadConfig download;
    public final UploadConfig upload;
    public final RegionConfig region;

    public QiniuKodoFsConfig(Configuration conf, String namespace) {
        super(conf, namespace);
        this.region = region();
        this.useHttps = useHttps();
        this.auth = auth();
        this.download = download();
        this.upload = upload();
    }

    public QiniuKodoFsConfig(Configuration conf) {
        this(conf, "fs.qiniu");
    }


    /**
     * 获取bucket的region配置信息
     */
    private RegionConfig region() {
        return new RegionConfig(conf, namespace + ".region");
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

    private UploadConfig upload() {
        return new UploadConfig(conf, namespace + ".upload");
    }

    @Override
    public String toString() {
        return "QiniuKodoFsConfig{" +
                ", useHttps=" + useHttps +
                ", auth=" + auth +
                ", download=" + download +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
