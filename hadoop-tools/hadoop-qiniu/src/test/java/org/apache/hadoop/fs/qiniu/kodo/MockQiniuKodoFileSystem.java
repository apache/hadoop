package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.fs.qiniu.kodo.download.blockreader.QiniuKodoGeneralBlockReader;

import java.io.IOException;
import java.net.URI;

public class MockQiniuKodoFileSystem extends QiniuKodoFileSystem {
    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        this.fsConfig = new QiniuKodoFsConfig(getConf());
        setLog4jConfig(fsConfig);

        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

        // 构造工作目录路径，工作目录路径为用户使用相对目录时所相对的路径
        this.username = "mockUser";
        this.workingDir = new Path("/user", username).makeQualified(uri, null);

        this.kodoClient = new MockQiniuKodoClient();
        this.generalblockReader = new QiniuKodoGeneralBlockReader(fsConfig, kodoClient);
    }
}
