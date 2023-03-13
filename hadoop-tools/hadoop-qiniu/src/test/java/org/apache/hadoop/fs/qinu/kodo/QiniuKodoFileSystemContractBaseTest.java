package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Before;

import java.net.URI;

public class QiniuKodoFileSystemContractBaseTest extends FileSystemContractBaseTest {
    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);

    }

    /**
     * 从根目录递归地遍历太慢了，所以禁用它
     */
    @Override
    protected boolean rootDirTestEnabled() {
        return false;
    }


    /**
     * 方便调试不会超时，所以超时时间设置为int的最大值
     */
    @Override
    protected int getGlobalTimeout() {
        return Integer.MAX_VALUE;
    }
}
