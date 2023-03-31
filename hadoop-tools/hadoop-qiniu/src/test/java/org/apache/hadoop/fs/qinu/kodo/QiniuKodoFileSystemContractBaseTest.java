package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class QiniuKodoFileSystemContractBaseTest extends FileSystemContractBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemContractBaseTest.class);

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

    @Test
    @Override
    public void testOverWriteAndRead() throws Exception {
        int blockSize = getBlockSize();

        byte[] filedata1 = dataset(blockSize * 2, 'A', 26);
        byte[] filedata2 = dataset(blockSize * 2, 'a', 26);
        Path path = path("testOverWriteAndRead/file-overwrite");
        writeAndRead(path, filedata1, blockSize, true, false);
        writeAndRead(path, filedata2, blockSize, true, false);
        writeAndRead(path, filedata1, blockSize * 2, true, false);
        writeAndRead(path, filedata2, blockSize * 2, true, false);
        writeAndRead(path, filedata1, blockSize, true, false);
        writeAndRead(path, filedata2, blockSize * 2, true, false);
    }

    /**
     * 方便调试不会超时，所以超时时间设置为int的最大值
     */
    @Override
    protected int getGlobalTimeout() {
        return Integer.MAX_VALUE;
    }
}
