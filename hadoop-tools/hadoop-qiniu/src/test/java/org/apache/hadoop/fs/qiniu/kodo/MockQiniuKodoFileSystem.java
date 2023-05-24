package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;

public class MockQiniuKodoFileSystem extends QiniuKodoFileSystem {
    @Override
    protected IQiniuKodoClient buildKodoClient(String bucket, QiniuKodoFsConfig fsConfig) {
        return new MockQiniuKodoClient();
    }
}
