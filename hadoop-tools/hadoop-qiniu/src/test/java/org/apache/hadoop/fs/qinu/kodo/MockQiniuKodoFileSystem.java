package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoCachedClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.download.blockreader.QiniuKodoGeneralBlockReader;
import org.apache.hadoop.fs.qiniu.kodo.download.blockreader.QiniuKodoRandomBlockReader;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class MockQiniuKodoFileSystem extends QiniuKodoFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(MockQiniuKodoFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        LOG.debug("initialize MockQiniuKodoFileSystem with uri: {}", name);
        setConf(conf);
        this.fsConfig = new QiniuKodoFsConfig(getConf());
        setLog4jConfig(fsConfig);

        String bucket = name.getHost();
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        this.username = UserGroupInformation.getCurrentUser().getShortUserName();
        this.workingDir = new Path("/user", username).makeQualified(uri, null);

        if (fsConfig.client.cache.enable) {
            this.kodoClient = new MockQiniuKodoClient();
            this.kodoClient = new QiniuKodoCachedClient(this.kodoClient, fsConfig.client.cache.maxCapacity);
        } else {
            this.kodoClient = new QiniuKodoClient(bucket, fsConfig, statistics);
        }

        this.generalblockReader = new QiniuKodoGeneralBlockReader(fsConfig, kodoClient);
        this.randomBlockReader = new QiniuKodoRandomBlockReader(
                kodoClient,
                fsConfig.download.random.blockSize,
                fsConfig.download.random.maxBlocks
        );
    }
}
